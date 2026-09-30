//! Persistent per-source session records — the single source of truth for
//! what the local stores contain.
//!
//! `store.discover()` reads every session file — Claude Code alone parses
//! each `.jsonl` in full — which made every scan take seconds. This module
//! keeps one record per session source (metadata, freshness, and a stat
//! fingerprint) in a small JSON file. A diff performs a stat-only walk of
//! every store location in `sources::sites()`; a source's body is only read
//! when it is new or its fingerprint moved, so steady-state diffs do no
//! transcript I/O at all.
//!
//! Both consumers read these records: ingest uploads a revision for every
//! `Ready` record whose content hash is absent from the catalog, and the MCP
//! request path lists them merged with the remote archive. Database-backed
//! stores (Hermes, Cursor Desktop, OpenCode) reuse
//! `sources::discover_db_candidates` but only when the database file itself
//! changed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use txcript::HarnessId;

use crate::document::ArchiveDocument;
use crate::error::{Error, Result};
use crate::merge::{Freshness, Info};
use crate::repo_id::{self, SessionRepo};
use crate::sources::{self, Candidate, Kind, LoadFailure, Site};

const STATE_SCHEMA: u32 = 1;
const STATE_FILE: &str = "local-state.json";
/// Failed origin resolutions are retried at most this often.
const REPO_RETRY: Duration = Duration::minutes(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordState {
    /// Body read, content hash computed.
    Ready,
    /// Body read but the repository could not be resolved, so no content
    /// hash exists yet. Retried when the cwd starts resolving.
    Pending,
    /// The source could not be parsed; retried only when it changes again.
    Broken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    pub harness: HarnessId,
    pub source: String,
    pub fingerprint: String,
    pub session_id: String,
    pub repo_key: Option<String>,
    pub state: RecordState,
    #[serde(flatten)]
    pub freshness: Freshness,
    #[serde(flatten)]
    pub info: Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepoEntry {
    repo_key: Option<String>,
    checked_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    schema: u32,
    /// Source records keyed by `"harness\nsource"`.
    #[serde(default)]
    sources: BTreeMap<String, SourceRecord>,
    /// cwd → repo key resolution. Failures are remembered with a timestamp
    /// so a non-git directory does not spawn `git` on every request.
    #[serde(default)]
    repos: BTreeMap<String, RepoEntry>,
    /// Per-harness database fingerprints for database-backed stores.
    #[serde(default)]
    db_fingerprints: BTreeMap<String, String>,
    /// Last orphan sweep. Sweeps are rate-limited so most ingests skip the
    /// bucket listing entirely.
    #[serde(default)]
    last_sweep: Option<DateTime<Utc>>,
}

impl State {
    fn empty() -> Self {
        Self {
            schema: STATE_SCHEMA,
            sources: BTreeMap::new(),
            repos: BTreeMap::new(),
            db_fingerprints: BTreeMap::new(),
            last_sweep: None,
        }
    }
}

/// What the last [`LocalStore::diff`] did.
#[derive(Debug, Default)]
pub struct Report {
    /// Bodies read this diff.
    pub loaded: usize,
    /// Records dropped because their source vanished.
    pub removed: usize,
    /// Loads that failed permanently this diff.
    pub broken: usize,
    /// Loads that failed transiently this diff.
    pub transient: usize,
    /// Freshly read sessions whose transcript records no usable cwd.
    pub missing_cwd: usize,
    /// Repositories whose sessions were added, changed, or removed this
    /// diff — the set ingest rebuilds search indexes for.
    pub changed_repos: BTreeSet<String>,
    /// Per-source failure details from this diff (`harness source — reason`).
    pub failures: Vec<String>,
}

pub struct LocalStore {
    path: PathBuf,
    state: State,
    dirty: bool,
    /// Diagnostics from the most recent `diff`.
    pub report: Report,
}

pub fn state_path(cache_dir: &Path) -> Result<PathBuf> {
    let dir = cache_dir
        .parent()
        .ok_or_else(|| Error::msg("cannot place the local state file"))?;
    Ok(dir.join(STATE_FILE))
}

impl LocalStore {
    /// Load the state file. A missing or corrupt file starts empty — the
    /// state is a cache and a full rescan rebuilds it.
    pub fn load(cache_dir: &Path) -> Self {
        let path = state_path(cache_dir).unwrap_or_else(|_| cache_dir.join(STATE_FILE));
        let state = match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<State>(&bytes) {
                Ok(state) if state.schema == STATE_SCHEMA => state,
                _ => State::empty(),
            },
            Err(_) => State::empty(),
        };
        // State files from earlier layouts.
        let _ = fs::remove_file(path.with_file_name("cursors.json"));
        let _ = fs::remove_dir_all(cache_dir.join("search"));
        Self {
            path,
            state,
            dirty: false,
            report: Report::default(),
        }
    }

    /// Persist the state when `diff` changed anything.
    pub fn save_if_dirty(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        crate::fsutil::atomic_write(&self.path, &serde_json::to_vec_pretty(&self.state)?)?;
        self.dirty = false;
        Ok(())
    }

    /// When the last orphan sweep ran — ingest rate-limits bucket listings.
    pub fn last_sweep(&self) -> Option<DateTime<Utc>> {
        self.state.last_sweep
    }

    /// Record a completed sweep.
    pub fn note_swept(&mut self) {
        self.state.last_sweep = Some(Utc::now());
        self.dirty = true;
    }

    pub fn records(&self) -> impl Iterator<Item = &SourceRecord> {
        self.state.sources.values()
    }

    /// Stat-only scan of every site in `sources::sites()`. Bodies are read
    /// only for new or changed sources.
    pub fn diff(&mut self) -> Result<()> {
        self.report = Report::default();
        for site in sources::sites() {
            match &site.kind {
                Kind::Files { .. } => self.scan_files(&site),
                Kind::SessionDirs { .. } | Kind::GrokBotDirs => self.scan_session_dirs(&site),
                Kind::Db => {
                    if let Err(error) = self.scan_db(&site) {
                        // A broken database store must not take down every
                        // query; its existing records stay until it recovers.
                        eprintln!(
                            "agent-transcript: {} discovery failed: {error}",
                            site.harness
                        );
                    }
                }
            }
        }
        // Sources whose stores disappeared entirely: drop their records for
        // any harness that no site covers anymore.
        self.drop_orphan_harnesses();
        self.resolve_pending();
        Ok(())
    }

    fn scan_files(&mut self, site: &Site) {
        let rows = sources::walk_tree(&site.path);
        let mut seen = HashSet::new();
        let root_str = site.path.to_string_lossy().into_owned();
        for (index, row) in rows.iter().enumerate() {
            if !site.kind.matches_file(&row.rel) {
                continue;
            }
            let source = sources::join_rel(&site.path, &row.rel)
                .to_string_lossy()
                .into_owned();
            let fingerprint = sources::row_fingerprint(index, &rows, site);
            let updated = sources::mtime_datetime(row.mtime_ns);
            seen.insert(source_key(site.harness, &source));
            self.reconsider(site.harness, &source, fingerprint, Some(updated));
        }
        self.drop_unseen(site.harness, Some(&root_str), &seen);
    }

    fn scan_session_dirs(&mut self, site: &Site) {
        let rows = sources::walk_tree(&site.path);
        let mut dirs = BTreeSet::new();
        for row in &rows {
            let Some((parent, name)) = row.rel.rsplit_once('/') else {
                continue;
            };
            if site.kind.is_dir_marker(parent, name) {
                dirs.insert(parent.to_string());
            }
        }
        let mut seen = HashSet::new();
        let root_str = site.path.to_string_lossy().into_owned();
        for dir in dirs {
            let prefix = format!("{dir}/");
            let members: Vec<&sources::TreeRow> = rows
                .iter()
                .filter(|row| row.rel.starts_with(&prefix))
                .collect();
            let fingerprint = sources::dir_fingerprint(members.iter().copied());
            let updated = members
                .iter()
                .map(|row| row.mtime_ns)
                .max()
                .map(sources::mtime_datetime);
            let source = sources::join_rel(&site.path, &dir)
                .to_string_lossy()
                .into_owned();
            seen.insert(source_key(site.harness, &source));
            self.reconsider(site.harness, &source, fingerprint, updated);
        }
        self.drop_unseen(site.harness, Some(&root_str), &seen);
    }

    fn scan_db(&mut self, site: &Site) -> Result<()> {
        let fingerprint = sources::db_fingerprint(&site.path);
        if self.state.db_fingerprints.get(site.harness.as_str()) == Some(&fingerprint) {
            return Ok(());
        }
        let candidates = sources::discover_db_candidates(site.harness)?;
        let mut seen = HashSet::new();
        for candidate in candidates {
            seen.insert(source_key(site.harness, &candidate.source));
            self.reconsider(site.harness, &candidate.source, candidate.fingerprint, None);
        }
        self.drop_unseen(site.harness, None, &seen);
        self.state
            .db_fingerprints
            .insert(site.harness.as_str().to_string(), fingerprint);
        self.dirty = true;
        Ok(())
    }

    /// Load or reload one source. Called for new sources and changed
    /// fingerprints; same-fingerprint records are left alone (pending
    /// retries are `resolve_pending`'s job).
    fn reconsider(
        &mut self,
        harness: HarnessId,
        source: &str,
        fingerprint: String,
        updated_at: Option<DateTime<Utc>>,
    ) {
        let key = source_key(harness, source);
        if self
            .state
            .sources
            .get(&key)
            .is_some_and(|record| record.fingerprint == fingerprint)
        {
            return;
        }
        self.reload(harness, source, fingerprint, updated_at);
    }

    fn reload(
        &mut self,
        harness: HarnessId,
        source: &str,
        fingerprint: String,
        updated_at: Option<DateTime<Utc>>,
    ) {
        let key = source_key(harness, source);
        // Failure details are reported once per body: a pending record that
        // stays pending across retries does not re-report.
        let already_pending = self.state.sources.get(&key).is_some_and(|record| {
            record.state == RecordState::Pending && record.fingerprint == fingerprint
        });
        let candidate = Candidate {
            harness,
            source: source.to_string(),
            fingerprint: fingerprint.clone(),
        };
        match sources::load(&candidate) {
            Ok(loaded) => {
                // `Transcript` is not `Clone`; capture the meta fields before
                // `ArchiveDocument::new` consumes it for the content hash.
                let transcript = loaded.transcript;
                let session_id = transcript.meta.id.clone();
                let cwd = transcript.meta.cwd.clone();
                let started_at = transcript.meta.timestamp;
                let freshness_meta = Freshness {
                    updated_at: updated_at.or(loaded.updated_at),
                    last_message_at: transcript.body.last().map(|message| message.timestamp),
                    message_count: transcript.body.len() as u64,
                    content_hash: String::new(),
                };
                let info = Info {
                    started_at,
                    title: transcript.meta.title.clone(),
                    cwd,
                    git_branch: transcript.meta.git_branch.clone(),
                    model: transcript.meta.model.clone(),
                };
                let (repo_key, content_hash, mut state) = match self
                    .resolve_repo(info.cwd.as_deref())
                {
                    SessionRepo::Key(repo_key) => {
                        let document = ArchiveDocument::new(harness, repo_key.clone(), transcript);
                        (
                            Some(repo_key),
                            document.content_hash().unwrap_or_default(),
                            RecordState::Ready,
                        )
                    }
                    SessionRepo::MissingCwd => {
                        if !already_pending {
                            self.report.missing_cwd += 1;
                        }
                        (None, String::new(), RecordState::Pending)
                    }
                    SessionRepo::Unresolved(reason) => {
                        if !already_pending {
                            self.report.failures.push(format!(
                                "{harness} {source} — cannot resolve origin: {reason}"
                            ));
                        }
                        (None, String::new(), RecordState::Pending)
                    }
                };
                if session_id.is_empty() {
                    state = RecordState::Broken;
                    self.report.broken += 1;
                    self.report
                        .failures
                        .push(format!("{harness} {source} — session id is empty"));
                }
                let freshness = Freshness {
                    content_hash,
                    ..freshness_meta
                };
                if state == RecordState::Ready {
                    if let Some(repo_key) = &repo_key {
                        self.report.changed_repos.insert(repo_key.clone());
                    }
                }
                let record = SourceRecord {
                    harness,
                    source: source.to_string(),
                    fingerprint,
                    session_id,
                    repo_key,
                    state,
                    freshness,
                    info,
                };
                self.state.sources.insert(key, record);
                self.report.loaded += 1;
                self.dirty = true;
            }
            Err(LoadFailure::Transient(message)) => {
                // Keep the previous record — the file is probably locked or
                // half-written; the next diff retries it.
                self.report.transient += 1;
                self.report
                    .failures
                    .push(format!("{harness} {source} — transient: {message}"));
            }
            Err(LoadFailure::Broken(message)) => {
                self.state.sources.insert(
                    key,
                    SourceRecord {
                        harness,
                        source: source.to_string(),
                        fingerprint,
                        session_id: String::new(),
                        repo_key: None,
                        state: RecordState::Broken,
                        freshness: Freshness {
                            updated_at,
                            last_message_at: None,
                            message_count: 0,
                            content_hash: String::new(),
                        },
                        info: Info {
                            started_at: Utc::now(),
                            title: None,
                            cwd: None,
                            git_branch: None,
                            model: None,
                        },
                    },
                );
                self.report.broken += 1;
                self.report
                    .failures
                    .push(format!("{harness} {source} — {message}"));
                self.dirty = true;
            }
        }
    }

    /// Repos are resolved once per cwd and cached in the state file;
    /// failures are retried every `REPO_RETRY` so a repo that gains an
    /// origin later picks up its pending sessions.
    fn resolve_repo(&mut self, cwd: Option<&str>) -> SessionRepo {
        let Some(cwd) = cwd.filter(|cwd| !cwd.is_empty()) else {
            return SessionRepo::MissingCwd;
        };
        if let Some(entry) = self.state.repos.get(cwd) {
            let fresh = Utc::now().signed_duration_since(entry.checked_at) < REPO_RETRY;
            if entry.repo_key.is_some() || fresh {
                return match &entry.repo_key {
                    Some(key) => SessionRepo::Key(key.clone()),
                    None => SessionRepo::Unresolved("cached".into()),
                };
            }
        }
        let resolved = match repo_id::session_repo(Some(cwd)) {
            Ok(resolved) => resolved,
            Err(error) => SessionRepo::Unresolved(error.to_string()),
        };
        let repo_key = match &resolved {
            SessionRepo::Key(key) => Some(key.clone()),
            _ => None,
        };
        self.state.repos.insert(
            cwd.to_string(),
            RepoEntry {
                repo_key,
                checked_at: Utc::now(),
            },
        );
        self.dirty = true;
        resolved
    }

    /// Records whose source vanished under this site's root are gone.
    fn drop_unseen(&mut self, harness: HarnessId, root: Option<&str>, seen: &HashSet<String>) {
        let prefix = format!("{}\n", harness.as_str());
        let mut removed_repos = Vec::new();
        let before = self.state.sources.len();
        self.state.sources.retain(|key, record| {
            let Some(rest) = key.strip_prefix(&prefix) else {
                return true;
            };
            if let Some(root) = root {
                if !rest.starts_with(root) {
                    return true;
                }
            }
            if seen.contains(key) {
                return true;
            }
            if let Some(repo_key) = &record.repo_key {
                removed_repos.push(repo_key.clone());
            }
            false
        });
        let removed = before - self.state.sources.len();
        if removed > 0 {
            self.report.removed += removed;
            self.report.changed_repos.extend(removed_repos);
            self.dirty = true;
        }
    }

    /// When a store root disappears entirely (harness uninstalled), its
    /// records are dropped on the next diff — sessions are files, so a
    /// missing store means missing sessions.
    fn drop_orphan_harnesses(&mut self) {
        let covered: HashSet<String> = sources::sites()
            .iter()
            .map(|site| site.harness.as_str().to_string())
            .collect();
        let mut removed_repos = Vec::new();
        let before = self.state.sources.len();
        self.state.sources.retain(|key, record| {
            let Some((harness, _)) = key.split_once('\n') else {
                return false;
            };
            if covered.contains(harness) {
                return true;
            }
            if let Some(repo_key) = &record.repo_key {
                removed_repos.push(repo_key.clone());
            }
            false
        });
        let removed = before - self.state.sources.len();
        if removed > 0 {
            self.report.removed += removed;
            self.report.changed_repos.extend(removed_repos);
            self.dirty = true;
        }
    }

    /// Pending records whose cwd now resolves get one body reload to compute
    /// their content hash. Cached failures are retried by `resolve_repo`'s
    /// own staleness bound.
    fn resolve_pending(&mut self) {
        let pending: Vec<String> = self
            .state
            .sources
            .iter()
            .filter(|(_, record)| record.state == RecordState::Pending)
            .map(|(key, _)| key.clone())
            .collect();
        for key in pending {
            let Some(record) = self.state.sources.get(&key) else {
                continue;
            };
            let harness = record.harness;
            let source = record.source.clone();
            let fingerprint = record.fingerprint.clone();
            let updated_at = record.freshness.updated_at;
            let cwd = record.info.cwd.clone();
            if matches!(self.resolve_repo(cwd.as_deref()), SessionRepo::Key(_)) {
                self.reload(harness, &source, fingerprint, updated_at);
            }
        }
    }
}

fn source_key(harness: HarnessId, source: &str) -> String {
    format!("{}\n{source}", harness.as_str())
}

/// Grouped session records ready for merge ranking — a lookup for
/// `sessions::local_views`.
impl LocalStore {
    pub fn grouped(&self) -> HashMap<(HarnessId, String, Option<String>), Vec<&SourceRecord>> {
        let mut groups: HashMap<(HarnessId, String, Option<String>), Vec<&SourceRecord>> =
            HashMap::new();
        for record in self.records() {
            if record.state == RecordState::Broken || record.session_id.is_empty() {
                continue;
            }
            groups
                .entry((
                    record.harness,
                    record.session_id.clone(),
                    record.repo_key.clone(),
                ))
                .or_default()
                .push(record);
        }
        groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::FileRule;

    #[test]
    fn file_rule_matches_extensions_and_cowork_records() {
        assert!(FileRule::Ext("jsonl").matches("a/b/session.jsonl"));
        assert!(!FileRule::Ext("jsonl").matches("a/b/session.json"));
        assert!(!FileRule::Ext("db").matches("store.db-wal"));
        assert!(FileRule::Ext("db").matches("chat/store.db"));
        assert!(FileRule::CoworkRecord.matches("org/acct/local_abc.json"));
        assert!(!FileRule::CoworkRecord.matches("org/acct/other.json"));
    }

    #[test]
    fn serde_round_trip_of_state() {
        let mut state = State::empty();
        state.sources.insert(
            "claude_code\n/tmp/a.jsonl".into(),
            SourceRecord {
                harness: HarnessId::ClaudeCode,
                source: "/tmp/a.jsonl".into(),
                fingerprint: "1:2".into(),
                session_id: "s1".into(),
                repo_key: Some("https://x/y".into()),
                state: RecordState::Ready,
                freshness: Freshness {
                    updated_at: None,
                    last_message_at: None,
                    message_count: 3,
                    content_hash: "abc".into(),
                },
                info: Info {
                    started_at: Utc::now(),
                    title: None,
                    cwd: None,
                    git_branch: None,
                    model: None,
                },
            },
        );
        let bytes = serde_json::to_vec(&state).unwrap();
        let back: State = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.sources.len(), 1);
    }
}
