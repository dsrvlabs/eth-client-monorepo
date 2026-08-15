//! Depth-4 Merkle inclusion proof for `blob_kzg_commitments` in `BeaconBlockBody`.

use cc_crypto::hash32_concat;
use cc_types::KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH;
use cc_types::block::BeaconBlockBody;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::sidecar::KzgCommitmentsInclusionProofDepth;
use ssz_types::FixedVector;
use tree_hash::TreeHash;

/// Field index of `blob_kzg_commitments` in Electra/Fulu `BeaconBlockBody`
/// (0-based, SSZ container order).
pub const BLOB_KZG_COMMITMENTS_FIELD_INDEX: usize = 11;

/// Number of body fields (Electra/Fulu).
const BODY_FIELD_COUNT: usize = 13;

/// Build the depth-4 inclusion proof for `body.blob_kzg_commitments`.
pub fn kzg_commitments_inclusion_proof<P: Preset>(
    body: &BeaconBlockBody<P>,
) -> FixedVector<Root, KzgCommitmentsInclusionProofDepth> {
    let leaves = body_field_roots(body);
    let branch = merkle_branch(&leaves, BLOB_KZG_COMMITMENTS_FIELD_INDEX, DEPTH);
    // SAFETY: DEPTH == 4 == KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH.
    debug_assert_eq!(DEPTH, KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize);
    FixedVector::new(branch).unwrap_or_else(|_| FixedVector::default())
}

/// Leaf root of `blob_kzg_commitments` (for `is_valid_merkle_branch`).
pub fn kzg_commitments_leaf<P: Preset>(body: &BeaconBlockBody<P>) -> Root {
    Root::from_hash256(body.blob_kzg_commitments.tree_hash_root())
}

const DEPTH: usize = 4;

fn body_field_roots<P: Preset>(body: &BeaconBlockBody<P>) -> Vec<[u8; 32]> {
    // Order must match BeaconBlockBody field declaration / tree_hash_derive.
    let roots = [
        body.randao_reveal.tree_hash_root(),
        body.eth1_data.tree_hash_root(),
        body.graffiti.tree_hash_root(),
        body.proposer_slashings.tree_hash_root(),
        body.attester_slashings.tree_hash_root(),
        body.attestations.tree_hash_root(),
        body.deposits.tree_hash_root(),
        body.voluntary_exits.tree_hash_root(),
        body.sync_aggregate.tree_hash_root(),
        body.execution_payload.tree_hash_root(),
        body.bls_to_execution_changes.tree_hash_root(),
        body.blob_kzg_commitments.tree_hash_root(),
        body.execution_requests.tree_hash_root(),
    ];
    debug_assert_eq!(roots.len(), BODY_FIELD_COUNT);
    roots
        .into_iter()
        .map(|h| {
            let mut a = [0u8; 32];
            a.copy_from_slice(h.as_slice());
            a
        })
        .collect()
}

/// Sibling path for leaf `index` in a depth-`depth` padded merkle tree.
fn merkle_branch(leaves: &[[u8; 32]], index: usize, depth: usize) -> Vec<Root> {
    let width = 1usize << depth;
    let mut layer: Vec<[u8; 32]> = vec![[0u8; 32]; width];
    for (i, leaf) in leaves.iter().enumerate().take(width) {
        layer[i] = *leaf;
    }

    let mut branch = Vec::with_capacity(depth);
    let mut idx = index;
    for _ in 0..depth {
        let sibling = idx ^ 1;
        branch.push(Root::from_array(layer[sibling]));
        // Parent layer.
        let mut next = vec![[0u8; 32]; layer.len() / 2];
        for i in 0..next.len() {
            next[i] = hash32_concat(&layer[2 * i], &layer[2 * i + 1]);
        }
        layer = next;
        idx /= 2;
    }
    branch
}

/// Verify proof against body root (wrapper around state-transition helper).
pub fn verify_inclusion_proof<P: Preset>(
    body: &BeaconBlockBody<P>,
    proof: &FixedVector<Root, KzgCommitmentsInclusionProofDepth>,
) -> bool {
    let leaf = kzg_commitments_leaf(body);
    let body_root = Root::from_hash256(body.tree_hash_root());
    let branch: Vec<Root> = proof.iter().copied().collect();
    cc_state_transition::helpers::misc::is_valid_merkle_branch(
        leaf,
        &branch,
        DEPTH,
        BLOB_KZG_COMMITMENTS_FIELD_INDEX as u64,
        body_root,
    )
}
