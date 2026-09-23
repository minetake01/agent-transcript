use std::collections::{BTreeMap, HashSet};
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
use crate::repo_id::{RepoCache, SessionRepo};
use crate::search_index;
use crate::sources::{self, Candidate, LoadFailure};
use crate::store::{Precondition, R2};
use txcript::HarnessId;

const CURSOR_SCHEMA: u32 = 2;
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
    changed_dirs: BTreeMap<String, PathBuf>,
    all_dirs: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CursorSet {
    schema: u32,
    sessions: BTreeMap<String, StoredCursor>,
    stores: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredCursor {
    fingerprint: String,
    content_hash: String,
    session_id: String,
    cwd: String,
    state: Keep,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Keep {
    Archived,
    Pending,
    Broken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Unchanged,
    Read,
}

pub async fn ingest() -> Result<()> {
    let _lock = RunLock::acquire()?;
    let config = config::load_config()?;
    config.require_write()?;
    let cursors_path = cursors_path()?;
    let cursors = load_cursors(&cursors_path)?;
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let scan = tokio::task::spawn_blocking(move || scan(cursors, catalog, key))
        .await
        .map_err(|error| Error::msg(format!("reading changed sessions: {error}")))??;
    for upload in &scan.uploads {
        r2.put(&upload.object_key, upload.blob.clone(), Precondition::None)
            .await?;
    }
    let index_key = config::load_key()?;
    commit_catalog(&r2, &index_key, &scan.incoming).await?;
    save_cursors(&cursors_path, &scan.cursors)?;

    // Rebuild changed repositories and initialize any repository that has no
    // durable search snapshot yet. This is deliberately outside the MCP
    // request path; the next search reads the resulting snapshot directly.
    let cache_dir = config::cache_dir()?;
    let mut index_dirs = scan.changed_dirs.clone();
    for (repo_key, directory) in &scan.all_dirs {
        if !index_dirs.contains_key(repo_key)
            && !search_index::local_path(&cache_dir, repo_key).is_file()
        {
            index_dirs.insert(repo_key.clone(), directory.clone());
        }
    }
    for directory in index_dirs.values() {
        if let Err(error) =
            search_index::build_for_directory(&r2, &index_key, &cache_dir, directory, true).await
        {
            eprintln!("agent-transcript: search index refresh failed: {error}");
        }
    }
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
    let _lock = RunLock::acquire()?;
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
    let live_indexes = catalog
        .sessions
        .iter()
        .map(|session| search_index::remote_key(&session.repo_key))
        .collect::<HashSet<_>>();
    for key in r2.list("v1/search/").await? {
        if !live_indexes.contains(&key) {
            r2.delete(&key).await?;
            deleted += 1;
        }
    }
    println!("deleted {deleted} unreferenced object(s)");
    Ok(())
}

fn scan(mut cursors: CursorSet, catalog: Catalog, key: Key) -> Result<Scan> {
    let collected = sources::collect(&cursors.stores)?;
    let mut known = catalog_index(&catalog);
    let mut repos = RepoCache::default();
    let mut changes = Vec::new();
    let mut unchanged = 0usize;
    let mut seen = HashSet::new();

    for candidate in collected.candidates {
        seen.insert(source_key(candidate.harness, &candidate.source));
        match step_of(&cursors, &known, &mut repos, &candidate)? {
            Step::Unchanged => unchanged += 1,
            Step::Read => changes.push(candidate),
        }
    }
    for harness in &collected.quiet {
        let prefix = format!("{}\n", harness.as_str());
        let owned: Vec<(String, StoredCursor)> = cursors
            .sessions
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(key, stored)| (key.clone(), stored.clone()))
            .collect();
        for (cursor_key, stored) in owned {
            seen.insert(cursor_key.clone());
            let source = cursor_key[prefix.len()..].to_string();
            let candidate = Candidate {
                harness: *harness,
                source,
                fingerprint: stored.fingerprint,
            };
            match step_of(&cursors, &known, &mut repos, &candidate)? {
                Step::Unchanged => unchanged += 1,
                Step::Read => changes.push(candidate),
            }
        }
    }
    cursors.sessions.retain(|key, _| {
        let Some((harness, _)) = split_key(key) else {
            return false;
        };
        if collected.quiet.contains(&harness) {
            return true;
        }
        if collected.generations.contains_key(harness.as_str()) {
            return seen.contains(key);
        }
        true
    });

    let mut scan = Scan {
        uploads: Vec::new(),
        incoming: Catalog::empty(),
        cursors,
        unchanged,
        read: 0,
        missing_cwd: 0,
        unresolved: Vec::new(),
        broken: Vec::new(),
        transient: Vec::new(),
        generations: collected.generations,
        changed_dirs: BTreeMap::new(),
        all_dirs: BTreeMap::new(),
    };
    for candidate in changes {
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
    collect_index_dirs(&mut scan)?;
    Ok(scan)
}

fn collect_index_dirs(scan: &mut Scan) -> Result<()> {
    let mut repos = RepoCache::default();
    for stored in scan.cursors.sessions.values() {
        let Some(cwd) = nonempty(&stored.cwd) else {
            continue;
        };
        let path = Path::new(cwd);
        if !path.is_dir() {
            continue;
        }
        if let SessionRepo::Key(repo_key) = repos.resolve(Some(cwd))? {
            scan.all_dirs
                .entry(repo_key)
                .or_insert_with(|| path.to_path_buf());
        }
    }
    Ok(())
}

fn step_of(
    cursors: &CursorSet,
    known: &HashSet<(HarnessId, String, String)>,
    repos: &mut RepoCache,
    candidate: &Candidate,
) -> Result<Step> {
    let Some(stored) = cursors
        .sessions
        .get(&source_key(candidate.harness, &candidate.source))
        .cloned()
    else {
        return Ok(Step::Read);
    };
    let pending_repo = if stored.state == Keep::Pending
        && !candidate.fingerprint.is_empty()
        && stored.fingerprint == candidate.fingerprint
    {
        Some(repos.resolve(nonempty(&stored.cwd))?)
    } else {
        None
    };
    let present = catalog_has(known, candidate.harness, &stored);
    Ok(step(
        Some(&stored),
        &candidate.fingerprint,
        present,
        pending_repo.as_ref(),
    ))
}

fn step(
    stored: Option<&StoredCursor>,
    fingerprint: &str,
    hash_in_catalog: bool,
    pending_repo: Option<&SessionRepo>,
) -> Step {
    let Some(stored) = stored else {
        return Step::Read;
    };
    if fingerprint.is_empty() || stored.fingerprint != fingerprint {
        return Step::Read;
    }
    match stored.state {
        Keep::Broken => Step::Unchanged,
        Keep::Archived if hash_in_catalog => Step::Unchanged,
        Keep::Archived => Step::Read,
        Keep::Pending => match pending_repo {
            Some(SessionRepo::Key(_)) => Step::Read,
            Some(SessionRepo::MissingCwd | SessionRepo::Unresolved(_)) => Step::Unchanged,
            None => Step::Read,
        },
    }
}

fn catalog_has(
    known: &HashSet<(HarnessId, String, String)>,
    harness: HarnessId,
    stored: &StoredCursor,
) -> bool {
    !stored.session_id.is_empty()
        && !stored.content_hash.is_empty()
        && known.contains(&(
            harness,
            stored.session_id.clone(),
            stored.content_hash.clone(),
        ))
}

fn nonempty(cwd: &str) -> Option<&str> {
    if cwd.is_empty() {
        None
    } else {
        Some(cwd)
    }
}

fn push_loaded(
    scan: &mut Scan,
    key: &Key,
    known: &mut HashSet<(HarnessId, String, String)>,
    repos: &mut RepoCache,
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
    match repos.resolve(nonempty(&cwd))? {
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
            if !cwd.is_empty() && Path::new(&cwd).is_dir() {
                scan.changed_dirs
                    .insert(repo_key.clone(), PathBuf::from(&cwd));
            }
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
    scan.cursors.sessions.insert(
        source_key(harness, source),
        StoredCursor {
            fingerprint: fingerprint.to_string(),
            content_hash: content_hash.to_string(),
            session_id: session_id.to_string(),
            cwd: cwd.to_string(),
            state,
        },
    );
}

fn remember_broken(scan: &mut Scan, candidate: &Candidate) {
    remember(
        scan,
        candidate.harness,
        "",
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
    let (name, source) = key.split_once('\n')?;
    Some((HarnessId::from_str(name).ok()?, source))
}

fn source_key(harness: HarnessId, source: &str) -> String {
    format!("{}\n{source}", harness.as_str())
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
            let value: serde_json::Value = serde_json::from_str(&text)?;
            let Some(schema) = value.get("schema").and_then(serde_json::Value::as_u64) else {
                return Err(Error::msg("ingest cursor is missing a schema"));
            };
            if schema != u64::from(CURSOR_SCHEMA) {
                let schema = u32::try_from(schema).unwrap_or(u32::MAX);
                return Err(Error::Schema { schema });
            }
            Ok(serde_json::from_value(value)?)
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

    fn stored(fingerprint: &str, hash: &str, state: Keep) -> StoredCursor {
        StoredCursor {
            fingerprint: fingerprint.into(),
            content_hash: hash.into(),
            session_id: "s1".into(),
            cwd: r"C:\repo".into(),
            state,
        }
    }

    #[test]
    fn archived_fingerprint_in_the_catalog_skips_the_body() {
        let cursor = stored("fp", "hash", Keep::Archived);
        assert_eq!(step(Some(&cursor), "fp", true, None), Step::Unchanged);
        assert_eq!(step(Some(&cursor), "fp", false, None), Step::Read);
        assert_eq!(step(Some(&cursor), "other", true, None), Step::Read);
    }

    #[test]
    fn empty_fingerprint_is_read() {
        let cursor = stored("fp", "hash", Keep::Archived);
        assert_eq!(step(Some(&cursor), "", true, None), Step::Read);
        assert_eq!(step(None, "fp", true, None), Step::Read);
    }

    #[test]
    fn pending_rereads_only_when_the_repo_resolves() {
        let cursor = stored("fp", "", Keep::Pending);
        let unresolved = SessionRepo::Unresolved("cannot resolve origin".into());
        assert_eq!(
            step(Some(&cursor), "fp", false, Some(&unresolved)),
            Step::Unchanged
        );
        assert_eq!(
            step(Some(&cursor), "fp", false, Some(&SessionRepo::MissingCwd)),
            Step::Unchanged
        );
        let resolved = SessionRepo::Key("https://github.com/Org/Repo".into());
        assert_eq!(
            step(Some(&cursor), "fp", false, Some(&resolved)),
            Step::Read
        );
    }

    #[test]
    fn broken_source_is_reread_only_when_its_fingerprint_changes() {
        let cursor = stored("fp", "", Keep::Broken);
        assert_eq!(step(Some(&cursor), "fp", false, None), Step::Unchanged);
        assert_eq!(step(Some(&cursor), "next", false, None), Step::Read);
    }

    #[test]
    fn source_key_is_the_harness_and_the_source() {
        assert_eq!(
            source_key(HarnessId::Codex, r"C:\sessions\s1"),
            "codex\nC:\\sessions\\s1"
        );
    }
}
