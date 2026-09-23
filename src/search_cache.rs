//! Bounded, disposable cache of txcript's extracted search lines.
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use txcript::search::{DocKey, Extracted};

use crate::merge::MergedView;

// Extracted's serialized representation is not stable across txcript versions.
const VERSION: &str = "txcript-0.14.4-search-v1";
const MAX_BYTES: u64 = 256 * 1024 * 1024;
const PLAINTEXT_MAX_BYTES: u64 = 512 * 1024 * 1024;
const MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

#[derive(Serialize, Deserialize)]
struct Entry {
    version: String,
    fingerprint: String,
    key: DocKey,
    extracted: Extracted,
}

#[derive(Serialize)]
struct EntryWrite<'a> {
    version: &'static str,
    fingerprint: &'a str,
    key: &'a DocKey,
    extracted: &'a Extracted,
}

fn path(dir: &Path, repo: &str, view: &MergedView) -> PathBuf {
    let digest =
        Sha256::digest(format!("{repo}\0{}\0{}", view.harness, view.session_id).as_bytes());
    dir.join("search")
        .join(format!("{}.json", hex::encode(digest)))
}

pub fn get(
    dir: &Path,
    repo: &str,
    view: &MergedView,
    fingerprint: &str,
    key: &DocKey,
) -> Option<Extracted> {
    let file = path(dir, repo, view);
    let bytes = fs::read(&file).ok()?;
    let entry: Entry = serde_json::from_slice(&bytes).ok()?;
    if entry.version != VERSION
        || entry.fingerprint != fingerprint
        || &entry.key != key
        || entry.extracted.key() != key
    {
        return None;
    }
    // Recency is based on use, not creation, so frequently searched repositories stay warm.
    if let Ok(handle) = fs::OpenOptions::new().write(true).open(&file) {
        let _ = handle.set_modified(SystemTime::now());
    }
    Some(entry.extracted)
}

pub fn put(
    dir: &Path,
    repo: &str,
    view: &MergedView,
    fingerprint: &str,
    key: DocKey,
    extracted: &Extracted,
) {
    let file = path(dir, repo, view);
    let entry = EntryWrite {
        version: VERSION,
        fingerprint,
        key: &key,
        extracted,
    };
    if let Ok(bytes) = serde_json::to_vec(&entry) {
        if let Some(parent) = file.parent() {
            if fs::create_dir_all(parent).is_ok() {
                // A temporary file prevents partially written entries from being read.
                let temp = parent.join(format!("{}.tmp", rand::random::<u64>()));
                if fs::write(&temp, bytes).is_ok() {
                    if fs::rename(&temp, &file).is_err() {
                        // Windows rename cannot overwrite an existing entry.
                        let _ = fs::remove_file(&file);
                        let _ = fs::rename(&temp, &file);
                    }
                }
                let _ = fs::remove_file(temp);
            }
        }
    }
}

/// Remove old entries and evict least recently used ones over the size budget.
/// Only files in our own search subdirectory are managed.
pub fn prune(dir: &Path) {
    prune_files(&dir.join("search"), MAX_BYTES, true);
}

pub fn prune_plaintext(dir: &Path) {
    prune_files(dir, PLAINTEXT_MAX_BYTES, false);
}

fn prune_files(dir: &Path, max_bytes: u64, search: bool) {
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
        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if path.extension().is_some_and(|ext| ext == "tmp") && search {
            if now.duration_since(modified).is_ok_and(|age| age > MAX_AGE) {
                let _ = fs::remove_file(path);
            }
            continue;
        }
        // Do not touch unknown files in the shared plaintext directory.
        if !path.extension().is_some_and(|ext| ext == "json")
            || (!search
                && !path.file_stem().is_some_and(|stem| {
                    let name = stem.to_string_lossy();
                    name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
                }))
        {
            continue;
        }
        if now.duration_since(modified).is_ok_and(|age| age > MAX_AGE) {
            let _ = fs::remove_file(path);
        } else {
            entries.push((path, modified, meta.len()));
        }
    }
    entries.sort_by_key(|(_, modified, _)| *modified);
    let mut total: u64 = entries.iter().map(|(_, _, size)| size).sum();
    for (path, _, size) in entries {
        if total <= max_bytes {
            break;
        }
        if fs::remove_file(path).is_ok() {
            total -= size;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::Pick;
    use chrono::Utc;
    use txcript::common::{Block, Message, Meta, Role};
    use txcript::search::{Index, Query};
    use txcript::{Common, HarnessId, Transcript};

    #[test]
    fn cached_extraction_survives_restart_and_rejects_stale_fingerprints() {
        let dir = tempfile::tempdir().unwrap();
        let timestamp = Utc::now();
        let view = MergedView {
            harness: HarnessId::Codex,
            session_id: "s1".into(),
            pick: Pick::Remote,
            started_at: timestamp,
            title: None,
            cwd: None,
            git_branch: None,
            model: None,
            sort_at: timestamp,
            object_key: None,
            content_hash: "hash".into(),
        };
        let key = DocKey {
            harness: view.harness,
            id: view.session_id.clone(),
            source: None,
        };
        let transcript = Transcript::<Common>::new(
            Meta {
                id: "s1".into(),
                timestamp,
                cwd: None,
                git_branch: None,
                title: None,
                cli_version: None,
                model: None,
            },
            vec![Message {
                role: Role::User,
                content: vec![Block::Text {
                    text: "needle".into(),
                }],
                timestamp,
                model: None,
                stop_reason: None,
                usage: None,
            }],
        );
        let extracted = Extracted::new(key.clone(), &transcript);
        put(dir.path(), "repo", &view, "first", key.clone(), &extracted);
        let mut index = Index::new();
        index.insert_extracted(get(dir.path(), "repo", &view, "first", &key).unwrap());
        assert_eq!(index.query(&Query::substring("needle")).len(), 1);
        assert!(get(dir.path(), "repo", &view, "second", &key).is_none());
        assert!(get(dir.path(), "other-repo", &view, "first", &key).is_none());
        fs::write(path(dir.path(), "repo", &view), b"broken").unwrap();
        assert!(get(dir.path(), "repo", &view, "first", &key).is_none());
    }
    #[test]
    fn prune_removes_old_and_excess() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("search");
        fs::create_dir(&folder).unwrap();
        fs::write(folder.join("old.json"), b"old").unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(folder.join("old.json"))
            .unwrap()
            .set_modified(SystemTime::now() - MAX_AGE - Duration::from_secs(1))
            .unwrap();
        fs::write(folder.join("new.json"), b"new").unwrap();
        prune(dir.path());
        assert!(!folder.join("old.json").exists());
        assert!(folder.join("new.json").exists());
        fs::write(folder.join("latest.json"), b"latest").unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(folder.join("new.json"))
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(60))
            .unwrap();
        prune_files(&folder, 6, true);
        assert!(!folder.join("new.json").exists());
        assert!(folder.join("latest.json").exists());
    }
}
