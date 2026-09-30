//! Filesystem helpers shared by the persistence paths.
use std::fs;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// Write `bytes` to `path` atomically: a temporary sibling is renamed over
/// the target so a reader never sees a partial file.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::msg(format!("{} has no parent directory", path.display())))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file"),
        rand::random::<u64>()
    ));
    fs::write(&temporary, bytes)?;
    if let Err(error) = fs::rename(&temporary, path) {
        // Windows cannot replace an existing file with rename.
        if path.exists() {
            fs::remove_file(path)?;
            fs::rename(&temporary, path)?;
        } else {
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
    }
    Ok(())
}

/// sha256 hex digest naming a repository key — the shared form for local
/// cache paths and R2 object names derived from a repo key.
pub fn repo_digest(repo_key: &str) -> String {
    hex::encode(Sha256::digest(repo_key.as_bytes()))
}
