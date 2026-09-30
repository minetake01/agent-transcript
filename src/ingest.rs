use std::collections::{BTreeSet, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::catalog::{merge_catalogs, object_key, Catalog, SessionRecord, SCHEMA};
use crate::config;
use crate::crypto::Key;
use crate::document::{encrypt_document, ArchiveDocument};
use crate::error::{Error, Result};
use crate::local_state::{LocalStore, RecordState};
use crate::remote::{commit_catalog, load_catalog};
use crate::search_index;
use crate::sources::{self, Candidate, LoadFailure};
use crate::store::{Precondition, R2};
use txcript::HarnessId;

const WATCH_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// How often ingest runs the orphan sweep and bucket-size report.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Objects younger than this are never deleted during a sweep — a
/// concurrent ingest on another machine may have already PUT the object but
/// not committed its catalog reference yet.
const SWEEP_GRACE: Duration = Duration::from_secs(600);

struct Upload {
    object_key: String,
    blob: Vec<u8>,
}

struct Scan {
    uploads: Vec<Upload>,
    incoming: Catalog,
    local: LocalStore,
    unchanged: usize,
    read: usize,
    missing_cwd: usize,
    failures: Vec<String>,
    /// Repositories whose sessions changed — their indexes are rebuilt.
    changed_repos: BTreeSet<String>,
    /// Every repository a local session belongs to — used to initialize
    /// repositories that have no snapshot yet.
    all_repos: BTreeSet<String>,
}

pub async fn ingest() -> Result<()> {
    let _lock = RunLock::acquire()?;
    let config = config::load_config()?;
    config.require_write()?;
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let cache_dir = config::cache_dir()?;
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let local = LocalStore::load(&cache_dir);
    let scan = tokio::task::spawn_blocking(move || scan(local, catalog, key))
        .await
        .map_err(|error| Error::msg(format!("reading changed sessions: {error}")))??;
    let Scan {
        uploads,
        incoming,
        mut local,
        unchanged,
        read,
        missing_cwd,
        failures,
        changed_repos,
        all_repos,
    } = scan;
    for upload in &uploads {
        r2.put(&upload.object_key, upload.blob.clone(), Precondition::None)
            .await?;
    }
    commit_catalog(&r2, &key, &incoming).await?;

    if sweep_due(local.last_sweep()) {
        match sweep(&r2, &key, &config).await {
            Ok(()) => {
                local.note_swept();
                if let Err(error) = local.save_if_dirty() {
                    eprintln!("agent-transcript: saving local state: {error}");
                }
            }
            Err(error) => eprintln!("agent-transcript: bucket sweep failed: {error}"),
        }
    }

    // Rebuild changed repositories and initialize any repository that has no
    // durable search snapshot yet. This is deliberately outside the MCP
    // request path; the next search reads the resulting snapshot directly.
    let mut index_repos = changed_repos;
    for repo_key in &all_repos {
        if !index_repos.contains(repo_key)
            && !search_index::local_path(&cache_dir, repo_key).is_file()
        {
            index_repos.insert(repo_key.clone());
        }
    }
    for repo_key in &index_repos {
        if let Err(error) =
            search_index::build_for_repo(&r2, &key, &cache_dir, repo_key, true, &mut local).await
        {
            eprintln!("agent-transcript: search index refresh failed: {error}");
        }
    }
    if let Err(error) = local.save_if_dirty() {
        eprintln!("agent-transcript: saving local state: {error}");
    }
    println!(
        "uploaded {} session(s), unchanged {}, read {}, missing cwd {}",
        uploads.len(),
        unchanged,
        read,
        missing_cwd
    );
    if !failures.is_empty() {
        return Err(Error::msg(format!(
            "{} source(s) could not be archived this run:\n{}",
            failures.len(),
            failures.join("\n")
        )));
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
    sweep(&r2, &key, &config).await
}

fn sweep_due(last: Option<DateTime<Utc>>) -> bool {
    match last {
        Some(last) => Utc::now()
            .signed_duration_since(last)
            .to_std()
            .map(|elapsed| elapsed >= SWEEP_INTERVAL)
            .unwrap_or(true),
        None => true,
    }
}

/// Delete objects no longer referenced by the catalog and report the bucket
/// footprint. Nothing is deleted for size alone; sessions are never dropped
/// to fit under `max_bucket_bytes`.
async fn sweep(r2: &R2, key: &Key, config: &config::Config) -> Result<()> {
    let (catalog, _) = load_catalog(r2, key).await?;
    let live = catalog
        .sessions
        .iter()
        .flat_map(|session| session.revisions.iter())
        .map(|revision| revision.object_key.clone())
        .collect::<HashSet<_>>();
    let live_indexes = catalog
        .sessions
        .iter()
        .map(|session| search_index::remote_key(&session.repo_key))
        .collect::<HashSet<_>>();
    let cutoff = Utc::now()
        - chrono::Duration::from_std(SWEEP_GRACE).unwrap_or_else(|_| chrono::Duration::minutes(10));
    let objects = r2.list_detailed("v1/").await?;
    let mut total = 0u64;
    let mut deleted = 0usize;
    for object in &objects {
        let orphan = is_orphan(&object.key, &live, &live_indexes);
        if orphan
            && object
                .last_modified
                .is_some_and(|modified| modified < cutoff)
        {
            r2.delete(&object.key).await?;
            deleted += 1;
            continue;
        }
        total += object.size;
    }
    println!(
        "bucket {} across {} object(s){}",
        fmt_bytes(total),
        objects.len() - deleted,
        if deleted > 0 {
            format!(", deleted {deleted} unreferenced")
        } else {
            String::new()
        }
    );
    let cap = config.bucket_cap();
    if total > cap {
        eprintln!(
            "agent-transcript: warning — bucket holds {}, above the {} \
             max_bucket_bytes target. Revisions are pruned automatically on \
             commit; reduce churn or raise max_bucket_bytes in config.toml.",
            fmt_bytes(total),
            fmt_bytes(cap)
        );
    }
    Ok(())
}

/// Whether a bucket object is no longer referenced by the catalog. The
/// catalog object itself is never an orphan.
fn is_orphan(key: &str, live: &HashSet<String>, live_indexes: &HashSet<String>) -> bool {
    match key.strip_prefix("v1/") {
        Some(rest) if rest.starts_with("objects/") => !live.contains(key),
        Some(rest) if rest.starts_with("search/") => !live_indexes.contains(key),
        _ => false,
    }
}

fn fmt_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Diff the local source records against the catalog: every `Ready` record
/// whose content hash is not already archived is re-read, encrypted, and
/// queued for upload.
fn scan(mut local: LocalStore, catalog: Catalog, key: Key) -> Result<Scan> {
    local.diff()?;
    // The records are the persistent state — they describe the filesystem,
    // not the upload outcome. A failed commit retries because the catalog
    // lacks the hash, not because a record forgot the body.
    local.save_if_dirty()?;

    let mut known = catalog_index(&catalog);
    let mut uploads = Vec::new();
    let mut incoming = Catalog::empty();
    let mut unchanged = 0usize;
    let mut failures = local.report.failures.clone();
    let mut changed_repos = local.report.changed_repos.clone();

    for record in local.records() {
        if record.state != RecordState::Ready {
            continue;
        }
        let Some(repo_key) = record.repo_key.clone() else {
            continue;
        };
        if known.contains(&(
            record.harness,
            record.session_id.clone(),
            record.freshness.content_hash.clone(),
        )) {
            unchanged += 1;
            continue;
        }
        let candidate = Candidate {
            harness: record.harness,
            source: record.source.clone(),
            fingerprint: record.fingerprint.clone(),
        };
        match sources::load(&candidate) {
            Ok(loaded) => {
                let session_id = loaded.transcript.meta.id.clone();
                if session_id.is_empty() {
                    failures.push(format!(
                        "{} {} — session id is empty",
                        record.harness, record.source
                    ));
                    continue;
                }
                let document =
                    ArchiveDocument::new(record.harness, repo_key.clone(), loaded.transcript);
                let hash = document.content_hash()?;
                if known.contains(&(record.harness, session_id.clone(), hash.clone())) {
                    // The file moved between the diff and this read, landing
                    // on a body the catalog already has.
                    unchanged += 1;
                    continue;
                }
                let object_key = object_key(&hash)?;
                let blob = encrypt_document(&key, &object_key, &document)?;
                let revision = document.revision(&hash, loaded.updated_at, blob.len() as u64)?;
                let piece = Catalog {
                    schema: SCHEMA,
                    sessions: vec![SessionRecord {
                        repo_key: repo_key.clone(),
                        harness: record.harness,
                        session_id: session_id.clone(),
                        revisions: vec![revision],
                    }],
                };
                incoming = merge_catalogs(&incoming, &piece)?;
                known.insert((record.harness, session_id, hash));
                uploads.push(Upload { object_key, blob });
                changed_repos.insert(repo_key);
            }
            Err(LoadFailure::Transient(message)) => {
                failures.push(format!(
                    "{} {} — transient: {message}",
                    record.harness, record.source
                ));
            }
            Err(LoadFailure::Broken(message)) => {
                failures.push(format!("{} {} — {message}", record.harness, record.source));
            }
        }
    }

    let all_repos = local
        .records()
        .filter_map(|record| record.repo_key.clone())
        .collect();
    let read = local.report.loaded;
    let missing_cwd = local.report.missing_cwd;
    Ok(Scan {
        uploads,
        incoming,
        local,
        unchanged,
        read,
        missing_cwd,
        failures,
        changed_repos,
        all_repos,
    })
}

fn catalog_index(catalog: &Catalog) -> HashSet<(HarnessId, String, String)> {
    let mut index = HashSet::new();
    for session in &catalog.sessions {
        for revision in &session.revisions {
            index.insert((
                session.harness,
                session.session_id.clone(),
                revision.freshness.content_hash.clone(),
            ));
        }
    }
    index
}

struct RunLock {
    path: PathBuf,
}

impl RunLock {
    fn acquire() -> Result<Self> {
        let path =
            crate::local_state::state_path(&config::cache_dir()?)?.with_file_name("ingest.lock");
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
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn setpriority(which: i32, who: u32, prio: i32) -> i32;
        }
        const PRIO_PROCESS: i32 = 0;
        // nice +10: the periodic archive pass stays out of interactive work.
        setpriority(PRIO_PROCESS, 0, 10);
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

    #[test]
    fn orphan_detection_keeps_live_objects_and_index_snapshots() {
        let live: HashSet<String> = ["v1/objects/sha256/aa/rest".to_string()]
            .into_iter()
            .collect();
        let indexes: HashSet<String> = ["v1/search/deadbeef".to_string()].into_iter().collect();
        assert!(!is_orphan("v1/objects/sha256/aa/rest", &live, &indexes));
        assert!(is_orphan("v1/objects/sha256/bb/rest", &live, &indexes));
        assert!(!is_orphan("v1/search/deadbeef", &live, &indexes));
        assert!(is_orphan("v1/search/ffff", &live, &indexes));
        // The catalog and anything outside v1/ is never an orphan.
        assert!(!is_orphan("v1/catalog", &live, &indexes));
        assert!(!is_orphan("v2/objects/x", &live, &indexes));
    }
}
