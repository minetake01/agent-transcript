use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::catalog::{Catalog, SessionRecord, SCHEMA};
use crate::config;
use crate::crypto::Key;
use crate::document::{encrypt_document, ArchiveDocument};
use crate::error::{Error, Result};
use crate::remote::{commit_catalog, load_catalog};
use crate::repo_id::{session_repo, SessionRepo};
use crate::store::{Precondition, R2};
use txcript::local::{self, Session};
use txcript::HarnessId;

const CURSOR_SCHEMA: u32 = 1;

struct Upload {
    object_key: String,
    blob: Vec<u8>,
}

struct Scan {
    uploads: Vec<Upload>,
    incoming: Catalog,
    cursors: CursorSet,
    unchanged: usize,
    read: usize,
    missing_cwd: usize,
    unresolved: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CursorSet {
    schema: u32,
    sessions: BTreeMap<String, StoredCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredCursor {
    fingerprint: String,
    content_hash: String,
}

pub async fn ingest() -> Result<()> {
    let config = config::load_config()?;
    config.require_write()?;
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let cursors_path = cursors_path()?;
    let cursors = load_cursors(&cursors_path)?;
    let scan = tokio::task::spawn_blocking(move || scan(catalog, key, cursors))
        .await
        .map_err(|error| Error::msg(format!("scanning local sessions: {error}")))??;
    for upload in &scan.uploads {
        r2.put(&upload.object_key, upload.blob.clone(), Precondition::None)
            .await?;
    }
    commit_catalog(&r2, &config::load_key()?, &scan.incoming).await?;
    save_cursors(&cursors_path, &scan.cursors)?;
    println!(
        "uploaded {} session(s), unchanged {}, read {}, missing cwd {}",
        scan.uploads.len(),
        scan.unchanged,
        scan.read,
        scan.missing_cwd
    );
    if !scan.unresolved.is_empty() {
        return Err(Error::msg(format!(
            "origin could not be resolved for {} session(s):\n{}",
            scan.unresolved.len(),
            scan.unresolved.join("\n")
        )));
    }
    Ok(())
}

pub async fn gc() -> Result<()> {
    let config = config::load_config()?;
    config.require_write()?;
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let mut live = std::collections::HashSet::new();
    for session in &catalog.sessions {
        for revision in &session.revisions {
            live.insert(revision.object_key.clone());
        }
    }
    let mut deleted = 0usize;
    for key in r2.list("v1/objects/").await? {
        if !live.contains(&key) {
            r2.delete(&key).await?;
            deleted += 1;
        }
    }
    println!("deleted {deleted} unreferenced object(s)");
    Ok(())
}

fn scan(catalog: Catalog, key: Key, cursors: CursorSet) -> Result<Scan> {
    let mut scan = Scan {
        uploads: Vec::new(),
        incoming: Catalog::empty(),
        cursors,
        unchanged: 0,
        read: 0,
        missing_cwd: 0,
        unresolved: Vec::new(),
    };
    let sessions: Vec<Session> = local::discover()
        .into_iter()
        .filter(|session| !matches!(session.harness, HarnessId::ClaudeChat | HarnessId::ChatGpt))
        .collect();
    let fingerprints = local::fingerprints(&sessions);
    if fingerprints.len() != sessions.len() {
        return Err(Error::msg("session fingerprints are misaligned"));
    }
    let mut repos = HashMap::<String, SessionRepo>::new();
    for (session, fingerprint) in sessions.into_iter().zip(fingerprints) {
        if archived(&scan, &catalog, session.harness, &session.meta.id, &fingerprint) {
            scan.unchanged += 1;
            continue;
        }
        match resolve_repo(session.meta.cwd.as_deref(), &mut repos)? {
            SessionRepo::MissingCwd => scan.missing_cwd += 1,
            SessionRepo::Unresolved(reason) => {
                scan.unresolved.push(format!(
                    "{} {} — {reason}",
                    session.harness, session.meta.id
                ));
            }
            SessionRepo::Key(repo_key) => {
                push_session(&mut scan, &catalog, &key, session, repo_key, &fingerprint)?
            }
        }
    }
    Ok(scan)
}

fn push_session(
    scan: &mut Scan,
    catalog: &Catalog,
    key: &Key,
    session: Session,
    repo_key: String,
    fingerprint: &str,
) -> Result<()> {
    let transcript = session.read().map_err(|error| {
        Error::msg(format!(
            "reading {} {}: {error}",
            session.harness, session.meta.id
        ))
    })?;
    scan.read += 1;
    let harness = session.harness;
    let session_id = session.meta.id.clone();
    let updated_at = session.updated_at;
    let document = ArchiveDocument::new(harness, repo_key.clone(), transcript);
    let hash = document.content_hash()?;
    remember(&mut scan.cursors, harness, &session_id, fingerprint, &hash);
    if known(catalog, &scan.incoming, harness, &session_id, &hash) {
        scan.unchanged += 1;
        return Ok(());
    }
    let revision = document.revision(&hash, updated_at)?;
    let object_key = revision.object_key.clone();
    let blob = encrypt_document(key, &object_key, &document)?;
    let piece = Catalog {
        schema: SCHEMA,
        sessions: vec![SessionRecord {
            repo_key,
            harness,
            session_id,
            revisions: vec![revision],
        }],
    };
    scan.incoming = crate::catalog::merge_catalogs(&scan.incoming, &piece)?;
    scan.uploads.push(Upload { object_key, blob });
    Ok(())
}

fn archived(
    scan: &Scan,
    catalog: &Catalog,
    harness: HarnessId,
    session_id: &str,
    fingerprint: &str,
) -> bool {
    if fingerprint.is_empty() {
        return false;
    }
    let Some(stored) = scan.cursors.sessions.get(&cursor_key(harness, session_id)) else {
        return false;
    };
    stored.fingerprint == fingerprint
        && known(
            catalog,
            &scan.incoming,
            harness,
            session_id,
            &stored.content_hash,
        )
}

fn remember(
    cursors: &mut CursorSet,
    harness: HarnessId,
    session_id: &str,
    fingerprint: &str,
    content_hash: &str,
) {
    if fingerprint.is_empty() {
        return;
    }
    cursors.sessions.insert(
        cursor_key(harness, session_id),
        StoredCursor {
            fingerprint: fingerprint.to_string(),
            content_hash: content_hash.to_string(),
        },
    );
}

fn cursor_key(harness: HarnessId, session_id: &str) -> String {
    format!("{}\n{session_id}", harness.as_str())
}

fn resolve_repo(cwd: Option<&str>, cache: &mut HashMap<String, SessionRepo>) -> Result<SessionRepo> {
    let Some(cwd) = cwd.filter(|cwd| !cwd.is_empty()) else {
        return Ok(SessionRepo::MissingCwd);
    };
    if let Some(resolved) = cache.get(cwd) {
        return Ok(resolved.clone());
    }
    let resolved = session_repo(Some(cwd))?;
    cache.insert(cwd.to_string(), resolved.clone());
    Ok(resolved)
}

fn cursors_path() -> Result<PathBuf> {
    let cache = config::cache_dir()?;
    let dir = cache
        .parent()
        .ok_or_else(|| Error::msg("cannot place the ingest cursor file"))?;
    Ok(dir.join("cursors.json"))
}

fn load_cursors(path: &Path) -> Result<CursorSet> {
    match fs::read_to_string(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(CursorSet::empty()),
        Err(error) => Err(error.into()),
        Ok(text) => {
            let cursors: CursorSet = serde_json::from_str(&text)?;
            if cursors.schema != CURSOR_SCHEMA {
                return Err(Error::Schema {
                    schema: cursors.schema,
                });
            }
            Ok(cursors)
        }
    }
}

fn save_cursors(path: &Path, cursors: &CursorSet) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_vec_pretty(cursors)?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, text)?;
    if path.exists() {
        fs::remove_file(path)?;
    }
    fs::rename(&temporary, path)?;
    Ok(())
}

impl CursorSet {
    fn empty() -> Self {
        Self {
            schema: CURSOR_SCHEMA,
            sessions: BTreeMap::new(),
        }
    }
}

fn known(
    catalog: &Catalog,
    incoming: &Catalog,
    harness: HarnessId,
    session_id: &str,
    hash: &str,
) -> bool {
    let contains = |source: &Catalog| {
        source.session(harness, session_id).is_some_and(|session| {
            session
                .revisions
                .iter()
                .any(|revision| revision.content_hash == hash)
        })
    };
    contains(catalog) || contains(incoming)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use txcript::HarnessId;

    use crate::catalog::{object_key, Catalog, Revision, SessionRecord, SCHEMA};

    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn catalog_with(hash: &str) -> Catalog {
        Catalog {
            schema: SCHEMA,
            sessions: vec![SessionRecord {
                repo_key: "https://github.com/Org/Repo".into(),
                harness: HarnessId::Codex,
                session_id: "s1".into(),
                revisions: vec![Revision {
                    content_hash: hash.into(),
                    object_key: object_key(hash).unwrap(),
                    title: None,
                    started_at: at(1),
                    updated_at: Some(at(2)),
                    last_message_at: None,
                    cwd: None,
                    git_branch: None,
                    model: None,
                    message_count: 1,
                }],
            }],
        }
    }

    fn stored(fingerprint: &str, hash: &str) -> CursorSet {
        let mut cursors = CursorSet::empty();
        remember(&mut cursors, HarnessId::Codex, "s1", fingerprint, hash);
        cursors
    }

    fn sample(cursors: CursorSet) -> Scan {
        Scan {
            uploads: Vec::new(),
            incoming: Catalog::empty(),
            cursors,
            unchanged: 0,
            read: 0,
            missing_cwd: 0,
            unresolved: Vec::new(),
        }
    }

    #[test]
    fn matching_fingerprint_skips_an_archived_session() {
        let scan = sample(stored("fp", "aa11"));
        assert!(archived(
            &scan,
            &catalog_with("aa11"),
            HarnessId::Codex,
            "s1",
            "fp"
        ));
    }

    #[test]
    fn empty_or_changed_fingerprint_is_read() {
        let scan = sample(stored("fp", "aa11"));
        let catalog = catalog_with("aa11");
        assert!(!archived(
            &scan, &catalog, HarnessId::Codex, "s1", ""
        ));
        assert!(!archived(
            &scan, &catalog, HarnessId::Codex, "s1", "other"
        ));
    }

    #[test]
    fn matching_fingerprint_without_the_catalog_hash_is_read() {
        let scan = sample(stored("fp", "aa11"));
        assert!(!archived(
            &scan,
            &Catalog::empty(),
            HarnessId::Codex,
            "s1",
            "fp"
        ));
    }
}
