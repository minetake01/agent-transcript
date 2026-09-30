use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::catalog::{merge_catalogs, validate, Catalog, CATALOG_KEY};
use crate::crypto::{self, Key};
use crate::document::ArchiveDocument;
use crate::error::{Error, Result};
use crate::fsutil::sha256_hex;
use crate::store::{Precondition, R2};

/// Decrypted object cache bounds: 512 MiB and 30 days.
const PLAINTEXT_MAX_BYTES: u64 = 512 * 1024 * 1024;
const PLAINTEXT_MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

pub async fn load_catalog(r2: &R2, key: &Key) -> Result<(Catalog, Option<String>)> {
    match r2.get(CATALOG_KEY).await? {
        None => Ok((Catalog::empty(), None)),
        Some(fetched) => {
            let plain = crypto::decrypt(key, CATALOG_KEY, &fetched.body)?;
            let catalog: Catalog = serde_json::from_slice(&plain)?;
            validate(&catalog)?;
            Ok((catalog, fetched.etag))
        }
    }
}

/// Merge `incoming` into the remote catalog with optimistic concurrency and
/// return the committed catalog — callers that follow with catalog-derived
/// work (search-index rebuilds) reuse it instead of fetching again.
pub async fn commit_catalog(r2: &R2, key: &Key, incoming: &Catalog) -> Result<Catalog> {
    if incoming.sessions.is_empty() {
        // Nothing to merge: the committed catalog is what is already remote.
        return Ok(load_catalog(r2, key).await?.0);
    }
    for attempt in 1..=5 {
        let (base, etag) = load_catalog(r2, key).await?;
        let mut merged = merge_catalogs(&base, incoming)?;
        crate::catalog::prune_to_current(&mut merged);
        let plain = serde_json::to_vec(&merged)?;
        let blob = crypto::encrypt(key, CATALOG_KEY, &plain)?;
        let precondition = match &etag {
            Some(etag) => Precondition::IfMatch(etag.clone()),
            None => Precondition::IfNoneMatchStar,
        };
        match r2.put(CATALOG_KEY, blob, precondition).await {
            Ok(()) => return Ok(merged),
            Err(Error::Precondition) if attempt < 5 => continue,
            Err(error) => return Err(error),
        }
    }
    Err(Error::msg("catalog update conflicted 5 times"))
}

pub async fn load_plaintext(
    r2: &R2,
    key: &Key,
    cache_dir: &Path,
    object_key: &str,
    content_hash: &str,
) -> Result<Vec<u8>> {
    let cached = cache_dir.join(format!("{content_hash}.json"));
    if let Ok(bytes) = fs::read(&cached) {
        if sha256_hex(&bytes) == content_hash {
            if let Ok(handle) = fs::OpenOptions::new().write(true).open(&cached) {
                let _ = handle.set_modified(std::time::SystemTime::now());
            }
            return Ok(bytes);
        }
        fs::remove_file(&cached)?;
    }
    let fetched = r2
        .get(object_key)
        .await?
        .ok_or_else(|| Error::msg(format!("catalog points at missing object `{object_key}`")))?;
    let plain = crypto::decrypt(key, object_key, &fetched.body)?;
    if sha256_hex(&plain) != content_hash {
        return Err(Error::msg(format!(
            "object `{object_key}` does not match catalog hash `{content_hash}`"
        )));
    }
    crate::fsutil::atomic_write(&cached, &plain)?;
    prune_plaintext_cache(cache_dir);
    Ok(plain)
}

/// Remove old plaintext objects and evict least recently used ones over the
/// size budget. Only the content-hash-named `.json` files this module writes
/// are touched.
pub fn prune_plaintext_cache(dir: &Path) {
    let Ok(files) = fs::read_dir(dir) else {
        return;
    };
    let now = SystemTime::now();
    let mut entries = Vec::new();
    for file in files.flatten() {
        let path = file.path();
        if !path.is_file() {
            continue;
        }
        let Ok(meta) = file.metadata() else { continue };
        // Only files this cache writes — a 64-hex-char content hash stem.
        if !path.extension().is_some_and(|ext| ext == "json")
            || !path.file_stem().is_some_and(|stem| {
                let name = stem.to_string_lossy();
                name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            continue;
        }
        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if now
            .duration_since(modified)
            .is_ok_and(|age| age > PLAINTEXT_MAX_AGE)
        {
            let _ = fs::remove_file(path);
        } else {
            entries.push((path, modified, meta.len()));
        }
    }
    entries.sort_by_key(|(_, modified, _)| *modified);
    let mut total: u64 = entries.iter().map(|(_, _, size)| size).sum();
    for (path, _, size) in entries {
        if total <= PLAINTEXT_MAX_BYTES {
            break;
        }
        if fs::remove_file(path).is_ok() {
            total -= size;
        }
    }
}

pub fn document_from_plaintext(bytes: &[u8]) -> Result<ArchiveDocument> {
    let document: ArchiveDocument = serde_json::from_slice(bytes)?;
    if document.schema != crate::document::SCHEMA {
        return Err(Error::Schema {
            schema: document.schema,
        });
    }
    Ok(document)
}
