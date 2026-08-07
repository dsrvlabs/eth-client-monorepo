//! Pre-generated signed block + data-column sidecar chain.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use cc_crypto::{
    compute_signing_root, get_domain, SecretKey, DOMAIN_BEACON_PROPOSER, DOMAIN_RANDAO,
    INFINITY_SIGNATURE,
};
use cc_state_transition::{get_beacon_proposer_index, get_current_epoch, get_randao_mix, process_block, process_slots,
    measured_canonical_root, TransitionContext,
};
use cc_types::block::{BeaconBlock, BeaconBlockBody, SignedBeaconBlock};
use cc_types::config::ChainConfig;
use cc_types::containers::{Eth1Data, SignedBeaconBlockHeader, SyncAggregate};
use cc_types::preset::Preset;
use cc_types::primitives::{BlsSignature, Domain, DomainType, Epoch, Root, Slot};
use cc_types::sidecar::DataColumnSidecar;
use cc_types::{BeaconState, NUMBER_OF_COLUMNS};
use ssz::Encode;
use ssz_types::VariableList;
use tree_hash::TreeHash;

use crate::inclusion::{kzg_commitments_inclusion_proof, verify_inclusion_proof};
use crate::keys::ValidatorKey;
use crate::kzg_columns::{columns_from_materials, compute_blob_materials, ColumnBundle};
use crate::params::DevnetParams;

/// Private always-Valid test harness (CC-32b: production stub deleted; not exported).
#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl<P: cc_types::preset::Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: cc_state_transition::NewPayloadRequest<'_, P>,
    ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
        Ok(cc_state_transition::PayloadStatus::Valid)
    }
}


/// One generated slot's artifacts (in memory).
#[derive(Debug, Clone)]
pub struct SlotArtifacts<P: Preset> {
    /// Slot number.
    pub slot: u64,
    /// Blob count in this block.
    pub blob_count: u64,
    /// Signed beacon block.
    pub block: SignedBeaconBlock<P>,
    /// Block root.
    pub block_root: Root,
    /// Column sidecars (128).
    pub sidecars: Vec<DataColumnSidecar<P>>,
    /// Column bundles (for KZG verify tests).
    pub columns: Vec<ColumnBundle>,
}

/// Generate the chain of signed blocks + sidecars, advancing `state` in place.
pub fn generate_chain<P: Preset>(
    state: &mut BeaconState<P>,
    params: &DevnetParams,
    keys: &[ValidatorKey],
    config: &ChainConfig,
    kzg: &impl cc_crypto::CellKzg,
    chain_dir: &Path,
) -> Result<Vec<SlotArtifacts<P>>> {
    fs::create_dir_all(chain_dir).with_context(|| format!("mkdir {}", chain_dir.display()))?;
    let engine = AcceptEngine;
    let ctx = TransitionContext::<P>::new(config, &engine);
    let seed = params.seed_bytes()?;
    let slots_per_epoch = P::SLOTS_PER_EPOCH;

    let mut artifacts = Vec::with_capacity(params.slot_count as usize);

    for slot_u in 1..=params.slot_count {
        let slot = Slot::new(slot_u);
        let epoch = slot_u / slots_per_epoch;
        let requested = params.blobs_for_slot(slot_u);
        let blob_count = params.capped_blobs_for_epoch(epoch, requested);

        // Advance empty slots (incl. epoch transitions).
        let pre_root = process_slots(state, slot)
            .map_err(|e| anyhow::anyhow!("process_slots to {slot_u}: {e:?}"))?;

        let proposer = get_beacon_proposer_index(state)
            .map_err(|e| anyhow::anyhow!("proposer at slot {slot_u}: {e:?}"))?;
        let proposer_idx = proposer.as_u64() as usize;
        let sk = &keys
            .get(proposer_idx)
            .with_context(|| format!("missing key for proposer {proposer_idx}"))?
            .secret;

        // Parent = hash_tree_root(latest_block_header) after process_slots.
        let parent_root =
            Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));

        // Blob / column material.
        let materials = compute_blob_materials(kzg, &seed, slot_u, blob_count)?;
        let commitments: Vec<_> = materials.iter().map(|m| m.commitment).collect();
        let columns = columns_from_materials(&materials);

        // Body.
        let randao_reveal = sign_randao(state, sk, slot)?;
        let prev_randao = get_randao_mix(state, get_current_epoch(state))
            .map_err(|e| anyhow::anyhow!("randao mix: {e:?}"))?;
        let timestamp = cc_state_transition::compute_time_at_slot(
            state.genesis_time(),
            slot,
            config.seconds_per_slot,
        );
        // Deterministic unique block hash per slot.
        let mut bh = [0u8; 32];
        bh[0..8].copy_from_slice(&slot_u.to_le_bytes());
        bh[8] = 0xb1;
        let blob_kzg_commitments = VariableList::new(commitments.clone())
            .map_err(|e| anyhow::anyhow!("blob_kzg_commitments: {e:?}"))?;
        let mut body = BeaconBlockBody::<P> {
            randao_reveal,
            eth1_data: Eth1Data {
                deposit_root: Root::ZERO,
                deposit_count: 0,
                block_hash: Root::from_array([0xee; 32]),
            },
            sync_aggregate: SyncAggregate {
                sync_committee_bits: Default::default(),
                sync_committee_signature: BlsSignature::from_array(INFINITY_SIGNATURE),
            },
            blob_kzg_commitments,
            ..Default::default()
        };
        body.execution_payload.parent_hash = state.latest_execution_payload_header().block_hash;
        body.execution_payload.prev_randao = prev_randao;
        body.execution_payload.timestamp = timestamp;
        body.execution_payload.block_number = slot_u;
        body.execution_payload.block_hash = Root::from_array(bh);

        let mut block = BeaconBlock {
            slot,
            proposer_index: proposer,
            parent_root,
            state_root: Root::ZERO,
            body,
        };

        // Apply block to compute post-state root (signatures checked later on final form).
        process_block(state, &block, &ctx, pre_root)
            .map_err(|e| anyhow::anyhow!("process_block slot {slot_u}: {e:?}"))?;
        let post_root = measured_canonical_root(state);
        block.state_root = post_root;

        // Sign final block (includes state_root).
        let signature = sign_block(state, sk, &block)?;
        let signed = SignedBeaconBlock {
            message: block,
            signature,
        };
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));

        // Inclusion proof + sidecars.
        let inclusion = kzg_commitments_inclusion_proof(&signed.message.body);
        debug_assert!(
            verify_inclusion_proof(&signed.message.body, &inclusion),
            "inclusion proof must verify for slot {slot_u}"
        );

        let signed_header = SignedBeaconBlockHeader {
            message: cc_types::containers::BeaconBlockHeader {
                slot: signed.message.slot,
                proposer_index: signed.message.proposer_index,
                parent_root: signed.message.parent_root,
                state_root: signed.message.state_root,
                body_root: Root::from_hash256(TreeHash::tree_hash_root(&signed.message.body)),
            },
            signature: signed.signature,
        };

        let mut sidecars = Vec::with_capacity(NUMBER_OF_COLUMNS as usize);
        for col in &columns {
            let sidecar = DataColumnSidecar {
                index: col.index,
                column: VariableList::new(col.cells.clone())
                    .map_err(|e| anyhow::anyhow!("column list: {e:?}"))?,
                kzg_commitments: VariableList::new(col.commitments.clone())
                    .map_err(|e| anyhow::anyhow!("commitments list: {e:?}"))?,
                kzg_proofs: VariableList::new(col.proofs.clone())
                    .map_err(|e| anyhow::anyhow!("proofs list: {e:?}"))?,
                signed_block_header: signed_header,
                kzg_commitments_inclusion_proof: inclusion.clone(),
            };
            sidecars.push(sidecar);
        }

        // Persist to disk.
        let slot_dir = chain_dir.join(format!("slot_{slot_u:06}"));
        fs::create_dir_all(&slot_dir)?;
        fs::write(
            slot_dir.join("block.ssz"),
            signed.as_ssz_bytes(),
        )?;
        for sc in &sidecars {
            fs::write(
                slot_dir.join(format!("column_{:03}.ssz", sc.index)),
                sc.as_ssz_bytes(),
            )?;
        }
        // Lightweight meta for publisher.
        let meta = serde_json::json!({
            "slot": slot_u,
            "proposer_index": proposer.as_u64(),
            "blob_count": blob_count,
            "block_root": format!("0x{}", hex::encode(block_root.as_slice())),
        });
        fs::write(slot_dir.join("meta.json"), serde_json::to_vec_pretty(&meta)?)?;

        artifacts.push(SlotArtifacts {
            slot: slot_u,
            blob_count,
            block: signed,
            block_root,
            sidecars,
            columns,
        });
    }

    Ok(artifacts)
}

fn domain_for_state<P: Preset>(
    state: &BeaconState<P>,
    domain_type: DomainType,
    epoch: Epoch,
) -> Domain {
    get_domain(
        &state.fork(),
        domain_type,
        Some(epoch),
        state.genesis_validators_root(),
    )
}

fn sign_randao<P: Preset>(
    state: &BeaconState<P>,
    sk: &SecretKey,
    slot: Slot,
) -> Result<BlsSignature> {
    let epoch = Epoch::new(slot.as_u64() / P::SLOTS_PER_EPOCH);
    let domain = domain_for_state(state, DOMAIN_RANDAO, epoch);
    let root = compute_signing_root(&epoch, domain);
    let sig = sk.sign(root.as_array());
    Ok(BlsSignature::from_array(sig.serialize()))
}

fn sign_block<P: Preset>(
    state: &BeaconState<P>,
    sk: &SecretKey,
    block: &BeaconBlock<P>,
) -> Result<BlsSignature> {
    let epoch = Epoch::new(block.slot.as_u64() / P::SLOTS_PER_EPOCH);
    let domain = domain_for_state(state, DOMAIN_BEACON_PROPOSER, epoch);
    let root = compute_signing_root(block, domain);
    let sig = sk.sign(root.as_array());
    Ok(BlsSignature::from_array(sig.serialize()))
}

/// Verify a block's proposer signature against the proposer's public key.
pub fn verify_block_proposer_sig<P: Preset>(
    state_fork: &cc_types::fork::Fork,
    gvr: Root,
    block: &SignedBeaconBlock<P>,
    proposer_pk: &cc_crypto::PublicKey,
) -> bool {
    let epoch = Epoch::new(block.message.slot.as_u64() / P::SLOTS_PER_EPOCH);
    let domain = get_domain(state_fork, DOMAIN_BEACON_PROPOSER, Some(epoch), gvr);
    let root = compute_signing_root(&block.message, domain);
    let Ok(sig) = cc_crypto::Signature::deserialize(block.signature.as_array()) else {
        return false;
    };
    cc_crypto::verify(proposer_pk, root.as_array(), &sig)
}

