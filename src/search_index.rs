//! Durable, per-repository search snapshots.
//!
//! The ordinary search cache stores one extracted transcript at a time. That is
//! useful while a snapshot is being built, but it still forces the MCP server
//! to rediscover every local session and to rebuild an in-memory index on every
//! query. A snapshot contains the complete searchable set for one repository
//! and can therefore be used directly by the query path.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use txcript::search::{DocKey, Extracted, Index};

use crate::crypto::{self, Key};
use crate::error::{Error, Result};
use crate::local_state::LocalStore;
use crate::merge::{MergedView, Pick};
use crate::repo_id;
use crate::search_cache;
use crate::sessions;
use crate::store::{Precondition, R2};

/// Version of the on-disk/R2 snapshot format.
///
/// `txcript::search::Extracted` deliberately has no serialization stability
/// guarantee, so the txcript version is part of this value.
pub const FORMAT: &str = "agent-transcript-search-v1-txcript-0.14.4";
/// Snapshot schema version.
pub const SCHEMA: u32 = 1;

const LOCAL_DIRECTORY: &str = "search-index";
const REMOTE_PREFIX: &str = "v1/search";

/// A complete search snapshot for one repository.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    schema: u32,
    format: String,
    repo_key: String,
    generation: String,
    catalog_etag: Option<String>,
    generated_at: DateTime<Utc>,
    documents: Vec<Document>,
}

#[derive(Serialize, Deserialize)]
struct Document {
    key: DocKey,
    fingerprint: String,
    extracted: Extracted,
}

/// An in-memory index built from a snapshot.
///
/// The index is kept behind an `Arc` by the MCP server so repeated searches do
/// not deserialize or rebuild it. `documents` mirrors `index` with the
/// serializable form of each doc so an updated runtime can be persisted back
/// into a snapshot (`Extracted` is consumed by `insert_extracted`).
pub struct Runtime {
    pub index: Index,
    repo_key: String,
    catalog_etag: Option<String>,
    generated_at: DateTime<Utc>,
    documents: HashMap<DocKey, DocEntry>,
    dirty: bool,
}

struct DocEntry {
    fingerprint: String,
    extracted: serde_json::Value,
}

impl Snapshot {
    /// Build a snapshot from merged views.
    ///
    /// This is intentionally the slow path: it may read local transcripts and
    /// fetch remote objects. Once it completes, all subsequent searches can use
    /// the resulting snapshot without doing that work again.
    pub async fn from_merged(
        repo_key: &str,
        catalog_etag: Option<String>,
        merged: &[MergedView],
        r2: &R2,
        encryption_key: &Key,
        cache_dir: &Path,
    ) -> Result<Self> {
        let mut documents = Vec::new();

        for view in merged {
            if view.pick == Pick::Ambiguous {
                continue;
            }
            let doc_key = DocKey {
                harness: view.harness,
                id: view.session_id.clone(),
                source: None,
            };
            let fingerprint = sessions::fingerprint_of(view);
            let extracted = if fingerprint.is_empty() {
                None
            } else {
                search_cache::get(cache_dir, repo_key, view, &fingerprint, &doc_key)
            };
            let extracted = match extracted {
                Some(extracted) => extracted,
                None => {
                    let transcript =
                        sessions::transcript(r2, encryption_key, cache_dir, view).await?;
                    let extracted = Extracted::new(doc_key.clone(), &transcript);
                    if !fingerprint.is_empty() {
                        search_cache::put(
                            cache_dir,
                            repo_key,
                            view,
                            &fingerprint,
                            doc_key.clone(),
                            &extracted,
                        );
                    }
                    extracted
                }
            };
            documents.push(Document {
                key: doc_key,
                fingerprint,
                extracted,
            });
        }

        documents.sort_by(|left, right| {
            left.key
                .harness
                .as_str()
                .cmp(right.key.harness.as_str())
                .then_with(|| left.key.id.cmp(&right.key.id))
                .then_with(|| left.key.source.cmp(&right.key.source))
        });
        let generation = generation(&documents)?;
        let snapshot = Self {
            schema: SCHEMA,
            format: FORMAT.to_string(),
            repo_key: repo_key.to_string(),
            generation,
            catalog_etag,
            generated_at: Utc::now(),
            documents,
        };
        save_local(cache_dir, &snapshot)?;
        // Per-document files are only a build accelerator. Keep their bounded
        // LRU behavior when a full snapshot is (re)built, never on the hot
        // query path.
        search_cache::prune(cache_dir);
        search_cache::prune_plaintext(cache_dir);
        Ok(snapshot)
    }

    /// Consume the snapshot and build the query index once.
    pub fn into_runtime(self) -> Runtime {
        let Snapshot {
            repo_key,
            catalog_etag,
            generated_at,
            documents,
            ..
        } = self;
        let mut index = Index::new();
        let mut map = HashMap::with_capacity(documents.len());
        for document in documents {
            let extracted =
                serde_json::to_value(&document.extracted).unwrap_or(serde_json::Value::Null);
            map.insert(
                document.key.clone(),
                DocEntry {
                    fingerprint: document.fingerprint,
                    extracted,
                },
            );
            index.insert_extracted(document.extracted);
        }
        Runtime {
            index,
            repo_key,
            catalog_etag,
            generated_at,
            documents: map,
            dirty: false,
        }
    }

    pub fn repo_key(&self) -> &str {
        &self.repo_key
    }

    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub fn generated_at(&self) -> DateTime<Utc> {
        self.generated_at
    }

    pub fn catalog_etag(&self) -> Option<&str> {
        self.catalog_etag.as_deref()
    }

    pub fn documents(&self) -> usize {
        self.documents.len()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    fn validate(&self, expected_repo: Option<&str>) -> Result<()> {
        if self.schema != SCHEMA {
            return Err(Error::Schema {
                schema: self.schema,
            });
        }
        if self.format != FORMAT {
            return Err(Error::msg(format!(
                "unsupported search index format `{}`",
                self.format
            )));
        }
        if let Some(expected_repo) = expected_repo {
            if self.repo_key != expected_repo {
                return Err(Error::msg(format!(
                    "search index belongs to `{}`, expected `{expected_repo}`",
                    self.repo_key
                )));
            }
        }
        for document in &self.documents {
            if document.extracted.key() != &document.key {
                return Err(Error::msg("search index document key is inconsistent"));
            }
        }
        Ok(())
    }
}

impl Runtime {
    /// An empty runtime for a repository with no snapshot anywhere yet —
    /// the first sync fills it incrementally.
    pub fn empty(repo_key: &str) -> Self {
        Self {
            index: Index::new(),
            repo_key: repo_key.to_string(),
            catalog_etag: None,
            generated_at: Utc::now(),
            documents: HashMap::new(),
            dirty: false,
        }
    }

    pub fn repo_key(&self) -> &str {
        &self.repo_key
    }

    pub fn generated_at(&self) -> DateTime<Utc> {
        self.generated_at
    }

    pub fn documents(&self) -> usize {
        self.documents.len()
    }

    pub fn fingerprint(&self, key: &DocKey) -> Option<&str> {
        self.documents
            .get(key)
            .map(|entry| entry.fingerprint.as_str())
    }

    pub fn keys(&self) -> impl Iterator<Item = &DocKey> {
        self.documents.keys()
    }

    pub fn upsert(&mut self, key: DocKey, fingerprint: String, extracted: Extracted) {
        let value = serde_json::to_value(&extracted).unwrap_or(serde_json::Value::Null);
        self.index.insert_extracted(extracted);
        self.documents.insert(
            key,
            DocEntry {
                fingerprint,
                extracted: value,
            },
        );
        self.dirty = true;
        self.generated_at = Utc::now();
    }

    pub fn remove(&mut self, key: &DocKey) {
        self.index.remove(key);
        if self.documents.remove(key).is_some() {
            self.dirty = true;
            self.generated_at = Utc::now();
        }
    }

    /// Rebuild a snapshot from the current documents so an incrementally
    /// updated runtime can be persisted again.
    pub fn to_snapshot(&self) -> Result<Snapshot> {
        let mut documents = Vec::with_capacity(self.documents.len());
        for (key, entry) in &self.documents {
            documents.push(Document {
                key: key.clone(),
                fingerprint: entry.fingerprint.clone(),
                extracted: serde_json::from_value(entry.extracted.clone())?,
            });
        }
        documents.sort_by(|left, right| {
            left.key
                .harness
                .as_str()
                .cmp(right.key.harness.as_str())
                .then_with(|| left.key.id.cmp(&right.key.id))
                .then_with(|| left.key.source.cmp(&right.key.source))
        });
        let generation = generation(&documents)?;
        Ok(Snapshot {
            schema: SCHEMA,
            format: FORMAT.to_string(),
            repo_key: self.repo_key.clone(),
            generation,
            catalog_etag: self.catalog_etag.clone(),
            generated_at: self.generated_at,
            documents,
        })
    }

    /// Persist when the runtime changed since the last save. Returns whether
    /// a snapshot was actually written.
    pub fn persist(&mut self, cache_dir: &Path) -> Result<bool> {
        if !self.dirty {
            return Ok(false);
        }
        save_local(cache_dir, &self.to_snapshot()?)?;
        self.dirty = false;
        Ok(true)
    }
}

/// Path of the local snapshot for a repository.
pub fn local_path(cache_dir: &Path, repo_key: &str) -> PathBuf {
    let digest = Sha256::digest(repo_key.as_bytes());
    cache_dir
        .join(LOCAL_DIRECTORY)
        .join(format!("{}.json", hex::encode(digest)))
}

/// Object key of the encrypted R2 snapshot for a repository.
pub fn remote_key(repo_key: &str) -> String {
    let digest = Sha256::digest(repo_key.as_bytes());
    format!("{REMOTE_PREFIX}/{}", hex::encode(digest))
}

/// Load and validate a local snapshot. A missing file is not an error.
pub fn load_local(cache_dir: &Path, repo_key: &str) -> Result<Option<Snapshot>> {
    let path = local_path(cache_dir, repo_key);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let snapshot: Snapshot = serde_json::from_slice(&bytes)?;
    snapshot.validate(Some(repo_key))?;
    Ok(Some(snapshot))
}

/// Save a snapshot locally using an atomic replacement.
pub fn save_local(cache_dir: &Path, snapshot: &Snapshot) -> Result<()> {
    snapshot.validate(Some(snapshot.repo_key()))?;
    atomic_write(
        &local_path(cache_dir, snapshot.repo_key()),
        &snapshot.to_bytes()?,
    )
}

/// Load an encrypted snapshot from R2 and cache it locally.
pub async fn load_remote(
    r2: &R2,
    key: &Key,
    cache_dir: &Path,
    repo_key: &str,
) -> Result<Option<Snapshot>> {
    let object = remote_key(repo_key);
    let Some(fetched) = r2.get(&object).await? else {
        return Ok(None);
    };
    let plain = crypto::decrypt(key, &object, &fetched.body)?;
    let snapshot: Snapshot = serde_json::from_slice(&plain)?;
    snapshot.validate(Some(repo_key))?;
    save_local(cache_dir, &snapshot)?;
    Ok(Some(snapshot))
}

/// Publish a snapshot as an encrypted R2 object.
///
/// R2 object replacement is atomic. A reader either sees the previous complete
/// object or the new complete object, never a partially written generation.
pub async fn publish_remote(r2: &R2, key: &Key, snapshot: &Snapshot) -> Result<()> {
    snapshot.validate(Some(snapshot.repo_key()))?;
    let plain = snapshot.to_bytes()?;
    publish_remote_bytes(r2, key, snapshot.repo_key(), &plain).await
}

/// Publish already serialized snapshot bytes. This is used by the MCP cold
/// path so it can schedule an upload without cloning the extracted documents.
pub async fn publish_remote_bytes(r2: &R2, key: &Key, repo_key: &str, plain: &[u8]) -> Result<()> {
    let object = remote_key(repo_key);
    let encrypted = crypto::encrypt(key, &object, plain)?;
    r2.put(&object, encrypted, Precondition::None).await
}

/// Build, cache, and optionally publish the index for one local directory.
pub async fn build_for_directory(
    r2: &R2,
    key: &Key,
    cache_dir: &Path,
    directory: &Path,
    publish: bool,
    local: &mut LocalStore,
) -> Result<Snapshot> {
    let repo_key = repo_id::origin_of(directory).map_err(|error| Error::msg(error.to_string()))?;
    let (catalog, catalog_etag) = crate::remote::load_catalog(r2, key).await?;
    local.diff()?;
    let locals = sessions::local_views(local)?;
    let merged = sessions::merged(&repo_key, None, &locals, &catalog)?;
    let snapshot =
        Snapshot::from_merged(&repo_key, catalog_etag, &merged, r2, key, cache_dir).await?;
    if publish {
        publish_remote(r2, key, &snapshot).await?;
    }
    Ok(snapshot)
}

/// Resolve a requested directory and build its complete index.
///
/// This is used by the explicit `index` command and by background maintenance.
pub async fn build_for_cwd(
    r2: &R2,
    key: &Key,
    cache_dir: &Path,
    cwd: Option<&str>,
    publish: bool,
    local: &mut LocalStore,
) -> Result<Snapshot> {
    let directory = requested_directory(cwd)?;
    build_for_directory(r2, key, cache_dir, &directory, publish, local).await
}

/// Download an already published index without scanning local stores.
///
/// This is primarily useful on a read-only PC: it makes the first query a
/// single R2 object fetch rather than a full local/R2 merge.
pub async fn download_for_cwd(
    r2: &R2,
    key: &Key,
    cache_dir: &Path,
    cwd: Option<&str>,
) -> Result<Option<Snapshot>> {
    let directory = requested_directory(cwd)?;
    let repo_key = repo_id::origin_of(&directory).map_err(|error| Error::msg(error.to_string()))?;
    load_remote(r2, key, cache_dir, &repo_key).await
}

fn requested_directory(cwd: Option<&str>) -> Result<PathBuf> {
    let process = std::env::current_dir()?;
    let directory = repo_id::scope_directory(cwd.map(Path::new), &process);
    if !directory.is_dir() {
        return Err(Error::msg(format!(
            "{} is not a directory",
            directory.display()
        )));
    }
    // Resolve once here so a malformed origin fails before the expensive scan.
    let _ = repo_id::origin_of(&directory).map_err(|error| Error::msg(error.to_string()))?;
    Ok(directory)
}

fn generation(documents: &[Document]) -> Result<String> {
    let mut hasher = Sha256::new();
    for document in documents {
        let bytes = serde_json::to_vec(document)?;
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::msg("search index path has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("snapshot"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use txcript::common::{Block, Message, Meta, Role};
    use txcript::search::{Case, Query};
    use txcript::HarnessId;
    use txcript::{Common, Transcript};

    fn extracted(harness: HarnessId, id: &str, text: &str) -> (DocKey, Extracted) {
        let timestamp = Utc::now();
        let key = DocKey {
            harness,
            id: id.into(),
            source: None,
        };
        let transcript = Transcript::<Common>::new(
            Meta {
                id: id.into(),
                timestamp,
                cwd: None,
                git_branch: None,
                title: None,
                cli_version: None,
                model: None,
            },
            vec![Message {
                role: Role::User,
                content: vec![Block::Text { text: text.into() }],
                timestamp,
                model: None,
                stop_reason: None,
                usage: None,
            }],
        );
        (key.clone(), Extracted::new(key, &transcript))
    }

    fn snapshot(repo: &str, docs: Vec<(DocKey, Extracted)>) -> Snapshot {
        let documents = docs
            .into_iter()
            .map(|(key, extracted)| Document {
                key,
                fingerprint: "fingerprint".into(),
                extracted,
            })
            .collect::<Vec<_>>();
        Snapshot {
            schema: SCHEMA,
            format: FORMAT.into(),
            repo_key: repo.into(),
            generation: generation(&documents).unwrap(),
            catalog_etag: Some("etag".into()),
            generated_at: Utc::now(),
            documents,
        }
    }

    #[test]
    fn local_snapshot_round_trips_and_rejects_other_repositories() {
        let dir = tempfile::tempdir().unwrap();
        let repo = "https://example.test/repo";
        let (key, extracted) = extracted(HarnessId::Codex, "s1", "needle");
        let value = snapshot(repo, vec![(key, extracted)]);
        save_local(dir.path(), &value).unwrap();
        let loaded = load_local(dir.path(), repo).unwrap().unwrap();
        assert_eq!(loaded.repo_key(), repo);
        assert_eq!(loaded.documents(), 1);
        assert!(load_local(dir.path(), "https://example.test/other")
            .unwrap()
            .is_none());
    }

    #[test]
    fn snapshot_generation_changes_when_content_changes() {
        let repo = "https://example.test/repo";
        let (key, first) = extracted(HarnessId::Codex, "s1", "first");
        let (key2, second) = extracted(HarnessId::Codex, "s1", "second");
        let first = snapshot(repo, vec![(key, first)]);
        let second = snapshot(repo, vec![(key2, second)]);
        assert_ne!(first.generation(), second.generation());
    }

    #[test]
    fn remote_key_is_stable_and_repo_specific() {
        assert_eq!(remote_key("repo"), remote_key("repo"));
        assert_ne!(remote_key("repo"), remote_key("other"));
    }

    #[test]
    fn runtime_upserts_removes_and_persists_incrementally() {
        let dir = tempfile::tempdir().unwrap();
        let repo = "https://example.test/repo";
        let mut runtime = Runtime::empty(repo);
        // Nothing changed — no file is written.
        assert!(!runtime.persist(dir.path()).unwrap());
        assert!(!local_path(dir.path(), repo).exists());

        let (key, extracted) = extracted(HarnessId::Codex, "s1", "needle");
        runtime.upsert(key.clone(), "fp1".into(), extracted);
        assert_eq!(runtime.fingerprint(&key), Some("fp1"));
        assert!(runtime.persist(dir.path()).unwrap());
        // A second persist with no changes writes nothing.
        assert!(!runtime.persist(dir.path()).unwrap());

        let loaded = load_local(dir.path(), repo).unwrap().unwrap();
        assert_eq!(loaded.documents(), 1);
        let mut runtime = loaded.into_runtime();
        assert_eq!(runtime.fingerprint(&key), Some("fp1"));
        runtime.remove(&key);
        assert_eq!(runtime.keys().count(), 0);
        assert!(runtime.persist(dir.path()).unwrap());
        assert_eq!(
            load_local(dir.path(), repo).unwrap().unwrap().documents(),
            0
        );
    }

    #[test]
    fn runtime_keeps_document_count() {
        let repo = "https://example.test/repo";
        let (key, extracted) = extracted(HarnessId::Codex, "s1", "needle");
        let runtime = snapshot(repo, vec![(key, extracted)]).into_runtime();
        assert_eq!(runtime.repo_key(), repo);
        assert_eq!(runtime.documents(), 1);
        let mut query = Query::substring("needle");
        query.case = Case::Insensitive;
        assert_eq!(runtime.index.query(&query).len(), 1);
    }
}
