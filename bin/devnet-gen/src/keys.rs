//! Deterministic BLS validator key material.

use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use cc_crypto::SecretKey;
use cc_types::primitives::BlsPublicKey;

/// One validator's secret + public material.
#[derive(Debug, Clone)]
pub struct ValidatorKey {
    /// Validator index.
    pub index: u64,
    /// Secret key.
    pub secret: SecretKey,
    /// Compressed public key bytes.
    pub pubkey: BlsPublicKey,
}

/// Derive `count` keys from `seed`.
pub fn derive_keys(seed: &[u8; 32], count: u64) -> Result<Vec<ValidatorKey>> {
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..count {
        let secret = SecretKey::from_seed_index(seed, i)
            .map_err(|e| anyhow::anyhow!("keygen index {i}: {e}"))?;
        let pk = secret.public_key().serialize();
        out.push(ValidatorKey {
            index: i,
            secret,
            pubkey: BlsPublicKey::from_array(pk),
        });
    }
    Ok(out)
}

/// Write `keys/validator_{i}.json` and a `keys/pubkeys.json` index.
///
/// Secret key files are created with mode `0o600` on Unix.
pub fn write_keys(dir: &Path, keys: &[ValidatorKey]) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
    let mut pubkeys = Vec::with_capacity(keys.len());
    for k in keys {
        let path = dir.join(format!("validator_{:05}.json", k.index));
        let sk_hex = hex::encode(k.secret.serialize());
        let pk_hex = hex::encode(k.pubkey.as_slice());
        let body = serde_json::json!({
            "index": k.index,
            "secret_key": sk_hex,
            "public_key": pk_hex,
        });
        write_secret_file(&path, &serde_json::to_vec_pretty(&body)?)
            .with_context(|| format!("write {}", path.display()))?;
        pubkeys.push(pk_hex);
    }
    let index_path = dir.join("pubkeys.json");
    // Pubkey index is not secret material; still restrict for consistency.
    write_secret_file(&index_path, &serde_json::to_vec_pretty(&pubkeys)?)
        .with_context(|| format!("write {}", index_path.display()))?;
    Ok(())
}

/// Create/truncate `path` and write `bytes`, preferring mode `0o600` on Unix.
fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        // Re-apply in case the file already existed with a looser mode.
        use std::os::unix::fs::PermissionsExt;
        let mut perms = f.metadata()?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;
    }
    #[cfg(not(unix))]
    {
        fs::write(path, bytes)?;
    }
    Ok(())
}
