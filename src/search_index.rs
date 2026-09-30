//! Durable, per-repository search snapshots.
//!
//! A snapshot contains the complete searchable set for one repository and is
//! served directly by the query path — the MCP server does not rediscover
//! sessions or rebuild an in-memory index per query. Full rebuilds reuse the
//! previous snapshot's extracted documents when a source's fingerprint is
//! unchanged.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use txcript::search::{DocKey, Extracted, Index};

use crate::crypto::{self, Key};
use crate::error::{Error, Result};
use crate::local_state::LocalStore;
use crate::merge::{MergedView, Pick};
use crate::repo_id;
use crate::sessions;
use crate::store::{Precondition, R2};

/// Version of the on-disk/R2 snapshot format.
///
/// `txcript::search::Extracted` deliberately has no serialization stability
/// guarantee, so the txcript version is part of this value. A test asserts
/// it stays in sync with the Cargo.toml dependency.
pub const FORMAT: &str = "agent-transcript-search-v1-txcript-0.14.4";
/// Snapshot schema version.
pub const SCHEMA: u32 = 1;

const LOCAL_DIRECTORY: &str = "search-index";
/// Snapshot namespace in the bucket, with trailing slash.
pub(crate) const SEARCH_PREFIX: &str = "v1/search/";

/// A complete search snapshot for one repository.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    schema: u32,
    format: String,
    repo_key: String,
    generation: String,
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
    /// fetch remote objects. Documents whose fingerprint is unchanged from
    /// the previous local snapshot are reused without re-reading them. Once
    /// the snapshot completes, all subsequent searches use it directly.
    pub async fn from_merged(
        repo_key: &str,
        merged: &[MergedView],
        r2: &R2,
        encryption_key: &Key,
        cache_dir: &Path,
    ) -> Result<Self> {
        // Seed from the previous snapshot: an unchanged (doc, fingerprint)
        // needs no transcript read at all.
        let mut prior: HashMap<(DocKey, String), Extracted> = load_local(cache_dir, repo_key)?
            .map(|snapshot| {
                snapshot
                    .documents
                    .into_iter()
                    .map(|document| ((document.key, document.fingerprint), document.extracted))
                    .collect()
            })
            .unwrap_or_default();
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
                prior.remove(&(doc_key.clone(), fingerprint.clone()))
            };
            let extracted = match extracted {
                Some(extracted) => extracted,
                None => {
                    let transcript =
                        sessions::transcript(r2, encryption_key, cache_dir, view).await?;
                    Extracted::new(doc_key.clone(), &transcript)
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
            documents,
        };
        save_local(cache_dir, &snapshot)?;
        crate::remote::prune_plaintext_cache(cache_dir);
        Ok(snapshot)
    }

    /// Consume the snapshot and build the query index once.
    pub fn into_runtime(self) -> Runtime {
        let Snapshot {
            repo_key,
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
            documents: HashMap::new(),
            dirty: false,
        }
    }

    pub fn repo_key(&self) -> &str {
        &self.repo_key
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
    }

    pub fn remove(&mut self, key: &DocKey) {
        self.index.remove(key);
        if self.documents.remove(key).is_some() {
            self.dirty = true;
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
    cache_dir
        .join(LOCAL_DIRECTORY)
        .join(format!("{}.json", crate::fsutil::repo_digest(repo_key)))
}

/// Object key of the encrypted R2 snapshot for a repository.
pub fn remote_key(repo_key: &str) -> String {
    format!("{SEARCH_PREFIX}{}", crate::fsutil::repo_digest(repo_key))
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
    crate::fsutil::atomic_write(
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

/// Build, cache, and optionally publish the index for one repository.
///
/// The caller supplies the already-scanned source state and the current
/// catalog: `diff` and `load_catalog` run once per operation, not once per
/// repository.
pub async fn build_for_repo(
    r2: &R2,
    key: &Key,
    cache_dir: &Path,
    repo_key: &str,
    publish: bool,
    local: &LocalStore,
    catalog: &crate::catalog::Catalog,
) -> Result<Snapshot> {
    let locals = sessions::local_views(local);
    let merged = sessions::merged(repo_key, None, &locals, catalog)?;
    let snapshot = Snapshot::from_merged(repo_key, &merged, r2, key, cache_dir).await?;
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
    process_dir: &Path,
    publish: bool,
    local: &mut LocalStore,
) -> Result<Snapshot> {
    let repo_key = requested_repo_key(cwd, process_dir, local)?;
    let (catalog, _) = crate::remote::load_catalog(r2, key).await?;
    local.diff()?;
    build_for_repo(r2, key, cache_dir, &repo_key, publish, local, &catalog).await
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
    process_dir: &Path,
    local: &mut LocalStore,
) -> Result<Option<Snapshot>> {
    let repo_key = requested_repo_key(cwd, process_dir, local)?;
    load_remote(r2, key, cache_dir, &repo_key).await
}

/// The `index` command: build the repository's snapshot on a read-write
/// install and publish it; on a read-only install fetch the published one,
/// falling back to a local build when none exists yet.
pub async fn index(cwd: Option<&str>, process_dir: &Path) -> Result<Snapshot> {
    let config = crate::config::load_config()?;
    let key = crate::config::load_key()?;
    let cache_dir = crate::config::cache_dir()?;
    let r2 = R2::new(&config);
    let mut local = LocalStore::load(&cache_dir);
    let snapshot = if config.can_write() {
        build_for_cwd(&r2, &key, &cache_dir, cwd, process_dir, true, &mut local).await?
    } else {
        match download_for_cwd(&r2, &key, &cache_dir, cwd, process_dir, &mut local).await? {
            Some(snapshot) => snapshot,
            None => {
                build_for_cwd(&r2, &key, &cache_dir, cwd, process_dir, false, &mut local).await?
            }
        }
    };
    if let Err(error) = local.save_if_dirty() {
        eprintln!("agent-transcript: saving local state: {error}");
    }
    Ok(snapshot)
}

/// Resolve a requested cwd to its repository key through the shared
/// resolution cache — the same normalization and origin probe every entry
/// point shares.
fn requested_repo_key(
    cwd: Option<&str>,
    process_dir: &Path,
    local: &mut LocalStore,
) -> Result<String> {
    let directory = repo_id::scope_directory(cwd.map(Path::new), process_dir);
    if !directory.is_dir() {
        return Err(Error::msg(format!(
            "{} is not a directory",
            directory.display()
        )));
    }
    local.resolve_directory(&directory).into_key(&directory)
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

    /// The format embeds the txcript version because `Extracted` has no
    /// serialization stability guarantee; it must track the Cargo.toml pin.
    #[test]
    fn format_names_the_pinned_txcript_version() {
        let version = FORMAT
            .rsplit_once("txcript-")
            .map(|(_, version)| version)
            .unwrap();
        let manifest =
            fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
        let dependency = manifest
            .lines()
            .find(|line| line.trim_start().starts_with("txcript"))
            .unwrap();
        assert!(
            dependency.contains(&format!("\"{version}\"")),
            "FORMAT names txcript {version}, Cargo.toml pins {dependency}"
        );
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
