use std::fs;
use std::path::Path;

use crate::catalog::{merge_catalogs, validate, Catalog, CATALOG_KEY};
use crate::crypto::{self, Key};
use crate::document::{hash_bytes, ArchiveDocument};
use crate::error::{Error, Result};
use crate::store::{Precondition, R2};

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

pub async fn commit_catalog(r2: &R2, key: &Key, incoming: &Catalog) -> Result<()> {
    if incoming.sessions.is_empty() {
        return Ok(());
    }
    for attempt in 1..=5 {
        let (base, etag) = load_catalog(r2, key).await?;
        let merged = merge_catalogs(&base, incoming)?;
        let plain = serde_json::to_vec(&merged)?;
        let blob = crypto::encrypt(key, CATALOG_KEY, &plain)?;
        let precondition = match &etag {
            Some(etag) => Precondition::IfMatch(etag.clone()),
            None => Precondition::IfNoneMatchStar,
        };
        match r2.put(CATALOG_KEY, blob, precondition).await {
            Ok(()) => return Ok(()),
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
        if hash_bytes(&bytes) == content_hash {
            return Ok(bytes);
        }
        fs::remove_file(&cached)?;
    }
    let fetched = r2
        .get(object_key)
        .await?
        .ok_or_else(|| Error::msg(format!("catalog points at missing object `{object_key}`")))?;
    let plain = crypto::decrypt(key, object_key, &fetched.body)?;
    if hash_bytes(&plain) != content_hash {
        return Err(Error::msg(format!(
            "object `{object_key}` does not match catalog hash `{content_hash}`"
        )));
    }
    fs::create_dir_all(cache_dir)?;
    fs::write(&cached, &plain)?;
    Ok(plain)
}

pub fn document_from_plaintext(bytes: &[u8]) -> Result<ArchiveDocument> {
    let document: ArchiveDocument = serde_json::from_slice(bytes)?;
    if document.schema != crate::catalog::SCHEMA {
        return Err(Error::Schema {
            schema: document.schema,
        });
    }
    Ok(document)
}
