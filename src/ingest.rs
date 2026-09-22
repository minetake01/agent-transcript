use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::catalog::{Catalog, SessionRecord, SCHEMA};
use crate::config;
use crate::crypto::Key;
use crate::document::{encrypt_document, ArchiveDocument};
use crate::error::{Error, Result};
use crate::remote::{commit_catalog, load_catalog};
use crate::repo_id::{session_repo, SessionRepo};
use crate::sources::{self, Candidate, LoadFailure};
use crate::store::{Precondition, R2};
use txcript::HarnessId;

const CURSOR_SCHEMA: u32 = 1;
const WATCH_INTERVAL: Duration = Duration::from_secs(5 * 60);

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
    broken: Vec<String>,
    transient: Vec<String>,
    generations: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CursorSet {
    schema: u32,
    sessions: BTreeMap<String, StoredCursor>,
    #[serde(default)]
    stores: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredCursor {
    fingerprint: String,
    content_hash: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    origin: String,
    #[serde(default)]
    state: Keep,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum Keep {
    #[default]
    Archived,
    Pending,
    Broken,
}

struct Prepared {
    cursors: CursorSet,
    unchanged: usize,
    changes: Vec<Candidate>,
    generations: BTreeMap<String, String>,
    adopted: bool,
}

pub async fn ingest() -> Result<()> {
    let _lock = RunLock::acquire()?;
    let config = config::load_config()?;
    config.require_write()?;
    let cursors_path = cursors_path()?;
    let cursors = load_cursors(&cursors_path)?;
    let prepared = tokio::task::spawn_blocking(move || prepare(cursors))
        .await
        .map_err(|error| Error::msg(format!("scanning local sessions: {error}")))??;
    if prepared.changes.is_empty() {
        finish_quiet(&cursors_path, prepared)?;
        return Ok(());
    }
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let scan = tokio::task::spawn_blocking(move || read_changes(prepared, catalog, key))
        .await
        .map_err(|error| Error::msg(format!("reading changed sessions: {error}")))??;
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
    if let Some(message) = failure_message(&scan) {
        return Err(Error::msg(message));
    }
    Ok(())
}

pub async fn watch() -> Result<()> {
    relax_priority();
    loop {
        if let Err(error) = ingest().await {
            eprintln!("agent-transcript: {error}");
        }
        tokio::time::sleep(WATCH_INTERVAL).await;
    }
}

pub async fn gc() -> Result<()> {
    let config = config::load_config()?;
    config.require_write()?;
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let mut live = HashSet::new();
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

fn finish_quiet(path: &Path, mut prepared: Prepared) -> Result<()> {
    let generations_changed = prepared
        .generations
        .iter()
        .any(|(key, generation)| prepared.cursors.stores.get(key.as_str()) != Some(generation));
    prepared.cursors.stores.extend(prepared.generations);
    if generations_changed || prepared.adopted {
        save_cursors(path, &prepared.cursors)?;
    }
    println!(
        "uploaded 0, unchanged {}, read 0, missing cwd 0",
        prepared.unchanged
    );
    Ok(())
}

fn prepare(mut cursors: CursorSet) -> Result<Prepared> {
    let collected = sources::collect(&cursors.stores)?;
    let quiet = quiet_count(&cursors, &collected.quiet);
    let (matched, changes, adopted) = decide(&collected.candidates, &mut cursors);
    Ok(Prepared {
        cursors,
        unchanged: quiet + matched,
        changes,
        generations: collected.generations,
        adopted,
    })
}

fn read_changes(prepared: Prepared, catalog: Catalog, key: Key) -> Result<Scan> {
    let mut scan = Scan {
        uploads: Vec::new(),
        incoming: Catalog::empty(),
        cursors: prepared.cursors,
        unchanged: prepared.unchanged,
        read: 0,
        missing_cwd: 0,
        unresolved: Vec::new(),
        broken: Vec::new(),
        transient: Vec::new(),
        generations: prepared.generations,
    };
    let mut known = catalog_index(&catalog);
    let mut repos = HashMap::<String, SessionRepo>::new();
    for candidate in prepared.changes {
        match sources::load(&candidate) {
            Ok(loaded) => push_loaded(&mut scan, &key, &mut known, &mut repos, &candidate, loaded)?,
            Err(LoadFailure::Broken(message)) => {
                remember_broken(&mut scan, &candidate);
                scan.broken.push(format!(
                    "{} {} — {message}",
                    candidate.harness, candidate.source
                ));
            }
            Err(LoadFailure::Transient(message)) => {
                scan.transient.push(format!(
                    "{} {} — {message}",
                    candidate.harness, candidate.source
                ));
            }
        }
    }
    if scan.transient.is_empty() {
        scan.cursors.stores.extend(scan.generations.clone());
    }
    Ok(scan)
}

fn push_loaded(
    scan: &mut Scan,
    key: &Key,
    known: &mut HashSet<(HarnessId, String, String)>,
    repos: &mut HashMap<String, SessionRepo>,
    candidate: &Candidate,
    loaded: sources::Loaded,
) -> Result<()> {
    let session_id = loaded.transcript.meta.id.clone();
    if session_id.is_empty() {
        remember_broken(scan, candidate);
        scan.broken.push(format!(
            "{} {} — session id is empty",
            candidate.harness, candidate.source
        ));
        return Ok(());
    }
    let cwd = loaded.transcript.meta.cwd.clone().unwrap_or_default();
    let updated_at = loaded.updated_at;
    let harness = candidate.harness;
    scan.read += 1;
    match resolve_repo(&cwd, repos)? {
        SessionRepo::MissingCwd => {
            remember(
                scan,
                harness,
                &session_id,
                &candidate.fingerprint,
                &candidate.source,
                &cwd,
                "",
                Keep::Pending,
            );
            scan.missing_cwd += 1;
        }
        SessionRepo::Unresolved(reason) => {
            remember(
                scan,
                harness,
                &session_id,
                &candidate.fingerprint,
                &candidate.source,
                &cwd,
                "",
                Keep::Pending,
            );
            scan.unresolved
                .push(format!("{harness} {session_id} — {reason}"));
        }
        SessionRepo::Key(repo_key) => {
            let document = ArchiveDocument::new(harness, repo_key.clone(), loaded.transcript);
            let hash = document.content_hash()?;
            if known.contains(&(harness, session_id.clone(), hash.clone())) {
                remember(
                    scan,
                    harness,
                    &session_id,
                    &candidate.fingerprint,
                    &candidate.source,
                    &cwd,
                    &hash,
                    Keep::Archived,
                );
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
                    session_id: session_id.clone(),
                    revisions: vec![revision],
                }],
            };
            scan.incoming = crate::catalog::merge_catalogs(&scan.incoming, &piece)?;
            known.insert((harness, session_id.clone(), hash.clone()));
            scan.uploads.push(Upload { object_key, blob });
            remember(
                scan,
                harness,
                &session_id,
                &candidate.fingerprint,
                &candidate.source,
                &cwd,
                &hash,
                Keep::Archived,
            );
        }
    }
    Ok(())
}

fn remember(
    scan: &mut Scan,
    harness: HarnessId,
    session_id: &str,
    fingerprint: &str,
    source: &str,
    cwd: &str,
    content_hash: &str,
    state: Keep,
) {
    let origin = if state == Keep::Pending {
        sources::origin_fingerprint(cwd)
    } else {
        String::new()
    };
    let key = cursor_key(harness, session_id);
    scan.cursors.sessions.retain(|existing, cursor| {
        existing == &key || cursor.source != source || source.is_empty()
    });
    scan.cursors.sessions.insert(
        key,
        StoredCursor {
            fingerprint: fingerprint.to_string(),
            content_hash: content_hash.to_string(),
            source: source.to_string(),
            cwd: cwd.to_string(),
            origin,
            state,
        },
    );
}

fn remember_broken(scan: &mut Scan, candidate: &Candidate) {
    remember(
        scan,
        candidate.harness,
        &candidate.source,
        &candidate.fingerprint,
        &candidate.source,
        "",
        "",
        Keep::Broken,
    );
}

fn failure_message(scan: &Scan) -> Option<String> {
    let mut parts = Vec::new();
    if !scan.unresolved.is_empty() {
        parts.push(format!(
            "origin could not be resolved for {} session(s):\n{}",
            scan.unresolved.len(),
            scan.unresolved.join("\n")
        ));
    }
    if !scan.broken.is_empty() {
        parts.push(format!(
            "skipped {} unreadable session(s):\n{}",
            scan.broken.len(),
            scan.broken.join("\n")
        ));
    }
    if !scan.transient.is_empty() {
        parts.push(format!(
            "will retry {} session(s):\n{}",
            scan.transient.len(),
            scan.transient.join("\n")
        ));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn decide(candidates: &[Candidate], cursors: &mut CursorSet) -> (usize, Vec<Candidate>, bool) {
    let mut by_source: HashMap<(HarnessId, String), String> = HashMap::new();
    let mut unbound: HashMap<(HarnessId, String), Vec<String>> = HashMap::new();
    for (key, stored) in &cursors.sessions {
        let Some((harness, _)) = split_key(key) else {
            continue;
        };
        if stored.source.is_empty() {
            if !stored.fingerprint.is_empty() {
                unbound
                    .entry((harness, stored.fingerprint.clone()))
                    .or_default()
                    .push(key.clone());
            }
        } else {
            by_source.insert((harness, stored.source.clone()), key.clone());
        }
    }
    let mut seen: HashMap<(HarnessId, String), usize> = HashMap::new();
    for candidate in candidates {
        if !candidate.fingerprint.is_empty() {
            *seen
                .entry((candidate.harness, candidate.fingerprint.clone()))
                .or_default() += 1;
        }
    }
    let mut unchanged = 0usize;
    let mut dirty = Vec::new();
    let mut adopted = false;
    for candidate in candidates {
        if let Some(key) = by_source
            .get(&(candidate.harness, candidate.source.clone()))
            .cloned()
        {
            let stored = &cursors.sessions[&key];
            let origin_now = if stored.state == Keep::Pending {
                sources::origin_fingerprint(&stored.cwd)
            } else {
                String::new()
            };
            if body_is_current(stored, &candidate.fingerprint, &origin_now) {
                unchanged += 1;
            } else {
                dirty.push(candidate.clone());
            }
            continue;
        }
        let fingerprint = (candidate.harness, candidate.fingerprint.clone());
        if !candidate.fingerprint.is_empty()
            && seen.get(&fingerprint) == Some(&1)
            && unbound
                .get(&fingerprint)
                .is_some_and(|keys| keys.len() == 1)
        {
            let key = unbound[&fingerprint][0].clone();
            if let Some(stored) = cursors.sessions.get_mut(&key) {
                stored.source = candidate.source.clone();
            }
            by_source.insert((candidate.harness, candidate.source.clone()), key);
            adopted = true;
            unchanged += 1;
            continue;
        }
        dirty.push(candidate.clone());
    }
    (unchanged, dirty, adopted)
}

fn body_is_current(stored: &StoredCursor, fingerprint: &str, origin_now: &str) -> bool {
    if stored.fingerprint != fingerprint {
        return false;
    }
    stored.state != Keep::Pending || stored.origin == origin_now
}

fn quiet_count(cursors: &CursorSet, quiet: &[HarnessId]) -> usize {
    cursors
        .sessions
        .keys()
        .filter(|key| split_key(key).is_some_and(|(harness, _)| quiet.contains(&harness)))
        .count()
}

fn catalog_index(catalog: &Catalog) -> HashSet<(HarnessId, String, String)> {
    let mut index = HashSet::new();
    for session in &catalog.sessions {
        for revision in &session.revisions {
            index.insert((
                session.harness,
                session.session_id.clone(),
                revision.content_hash.clone(),
            ));
        }
    }
    index
}

fn split_key(key: &str) -> Option<(HarnessId, &str)> {
    let (name, session_id) = key.split_once('\n')?;
    Some((HarnessId::from_str(name).ok()?, session_id))
}

fn cursor_key(harness: HarnessId, session_id: &str) -> String {
    format!("{}\n{session_id}", harness.as_str())
}

fn resolve_repo(cwd: &str, cache: &mut HashMap<String, SessionRepo>) -> Result<SessionRepo> {
    if cwd.is_empty() {
        return Ok(SessionRepo::MissingCwd);
    }
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
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(CursorSet::empty()),
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
            stores: BTreeMap::new(),
        }
    }
}

struct RunLock {
    path: PathBuf,
}

impl RunLock {
    fn acquire() -> Result<Self> {
        let path = cursors_path()?.with_file_name("ingest.lock");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Ok(text) = fs::read_to_string(&path) {
            if let Ok(pid) = text.trim().parse::<u32>() {
                if pid != std::process::id() && process_alive(pid) {
                    return Err(Error::msg("ingest is already running"));
                }
            }
            fs::remove_file(&path)?;
        }
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(file, "{}", std::process::id())?;
                Ok(Self { path })
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                Err(Error::msg("ingest is already running"))
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn relax_priority() {
    #[cfg(windows)]
    unsafe {
        unsafe extern "system" {
            fn GetCurrentProcess() -> *mut core::ffi::c_void;
            fn SetPriorityClass(process: *mut core::ffi::c_void, class: u32) -> i32;
        }
        const BELOW_NORMAL_PRIORITY_CLASS: u32 = 0x0000_4000;
        SetPriorityClass(GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS);
    }
    #[cfg(target_os = "macos")]
    unsafe {
        unsafe extern "C" {
            fn setpriority(which: i32, who: u32, prio: i32) -> i32;
        }
        const PRIO_DARWIN_PROCESS: i32 = 4;
        const PRIO_DARWIN_BG: i32 = 0x1000;
        setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG);
    }
}

fn process_alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        unsafe extern "system" {
            fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut core::ffi::c_void;
            fn CloseHandle(handle: *mut core::ffi::c_void) -> i32;
        }
        const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return false;
            }
            CloseHandle(handle);
            true
        }
    }
    #[cfg(unix)]
    {
        unsafe extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        const EPERM: i32 = 1;
        let Ok(pid) = i32::try_from(pid) else {
            return false;
        };
        if pid <= 0 {
            return false;
        }
        unsafe {
            if kill(pid, 0) == 0 {
                return true;
            }
        }
        std::io::Error::last_os_error().raw_os_error() == Some(EPERM)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(fingerprint: &str, state: Keep, origin: &str) -> StoredCursor {
        StoredCursor {
            fingerprint: fingerprint.into(),
            content_hash: "aa11".into(),
            source: String::new(),
            cwd: String::new(),
            origin: origin.into(),
            state,
        }
    }

    fn candidate(fingerprint: &str, source: &str) -> Candidate {
        Candidate {
            harness: HarnessId::Codex,
            source: source.into(),
            fingerprint: fingerprint.into(),
        }
    }

    #[test]
    fn archived_fingerprint_skips_the_body() {
        let cursor = stored("fp", Keep::Archived, "");
        assert!(body_is_current(&cursor, "fp", ""));
        assert!(!body_is_current(&cursor, "other", ""));
        let mut unmarked = stored("", Keep::Archived, "");
        unmarked.fingerprint.clear();
        assert!(body_is_current(&unmarked, "", ""));
    }

    #[test]
    fn pending_session_is_reread_only_when_its_origin_changes() {
        let cursor = stored("fp", Keep::Pending, "missing-cwd");
        assert!(body_is_current(&cursor, "fp", "missing-cwd"));
        assert!(!body_is_current(&cursor, "fp", "present"));
    }

    #[test]
    fn unique_legacy_fingerprint_is_adopted_without_a_read() {
        let mut cursors = CursorSet::empty();
        cursors.sessions.insert(
            cursor_key(HarnessId::Codex, "s1"),
            stored("fp", Keep::Archived, ""),
        );
        let (unchanged, dirty, adopted) =
            decide(&[candidate("fp", r"C:\sessions\s1")], &mut cursors);
        assert!(adopted);
        assert_eq!(unchanged, 1);
        assert!(dirty.is_empty());
        assert_eq!(
            cursors.sessions.values().next().unwrap().source,
            r"C:\sessions\s1"
        );
    }

    #[test]
    fn shared_legacy_fingerprint_is_read() {
        let mut cursors = CursorSet::empty();
        cursors.sessions.insert(
            cursor_key(HarnessId::Codex, "s1"),
            stored("fp", Keep::Archived, ""),
        );
        cursors.sessions.insert(
            cursor_key(HarnessId::Codex, "s2"),
            stored("fp", Keep::Archived, ""),
        );
        let (unchanged, dirty, _) = decide(&[candidate("fp", r"C:\sessions\s1")], &mut cursors);
        assert_eq!(unchanged, 0);
        assert_eq!(dirty.len(), 1);
    }

    #[test]
    fn empty_fingerprint_is_read() {
        let mut cursors = CursorSet::empty();
        let mut cursor = stored("fp", Keep::Archived, "");
        cursor.source = r"C:\sessions\s1".into();
        cursors
            .sessions
            .insert(cursor_key(HarnessId::Codex, "s1"), cursor);
        let (_, dirty, _) = decide(&[candidate("", r"C:\sessions\s1")], &mut cursors);
        assert_eq!(dirty.len(), 1);
    }
}
