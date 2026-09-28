//! Persistent per-source session records for the MCP request path.
//!
//! `local::discover()` reads every session file — Claude Code alone parses
//! each `.jsonl` in full — which made every MCP call take seconds. This
//! module keeps one record per session source (metadata, freshness, and a
//! stat fingerprint) in a small JSON file. Each call performs a stat-only
//! scan; a source's body is only read when it is new or its fingerprint
//! moved, so steady-state calls do no transcript I/O at all.
//!
//! Single-file stores (Claude Code, Codex, Pi, Campfire, Cursor, Amp,
//! Antigravity, Cowork) are enumerated straight from the file listing —
//! `store.discover()` is never needed. Directory-based sessions (Grok, fx,
//! Grok Bot) are detected by marker files inside their session directory.
//! Database-backed stores (Hermes, Cursor Desktop, OpenCode) reuse
//! `sources::discover_db_candidates` but only when the database file itself
//! changed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use txcript::harness::{
    amp, campfire, claude_code, codex, cowork, cursor, cursor_desktop, fx, grok, grok_bot, hermes,
    opencode, pi,
};
use txcript::HarnessId;

use crate::document::ArchiveDocument;
use crate::error::{Error, Result};
use crate::repo_id::{self, SessionRepo};
use crate::sources::{self, Candidate, LoadFailure, TreeRow};

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
    pub started_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub message_count: u64,
    pub content_hash: String,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub model: Option<String>,
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
}

impl State {
    fn empty() -> Self {
        Self {
            schema: STATE_SCHEMA,
            sources: BTreeMap::new(),
            repos: BTreeMap::new(),
            db_fingerprints: BTreeMap::new(),
        }
    }
}

/// What the last [`LocalStore::diff`] did.
#[derive(Debug, Default)]
pub struct Report {
    pub loaded: usize,
    pub removed: usize,
    pub broken: usize,
    pub transient: usize,
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
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = self.path.with_extension("json.tmp");
        fs::write(&temporary, serde_json::to_vec_pretty(&self.state)?)?;
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        fs::rename(&temporary, &self.path)?;
        self.dirty = false;
        Ok(())
    }

    pub fn records(&self) -> impl Iterator<Item = &SourceRecord> {
        self.state.sources.values()
    }

    /// Stat-only scan of every store. Bodies are read only for new or
    /// changed sources.
    pub fn diff(&mut self) -> Result<()> {
        self.report = Report::default();
        for probe in probes() {
            match probe {
                Probe::Files {
                    harness,
                    root,
                    rule,
                } => {
                    self.scan_files(harness, &root, rule);
                }
                Probe::SessionDirs {
                    harness,
                    root,
                    markers,
                } => {
                    self.scan_session_dirs(harness, &root, markers, false);
                }
                Probe::GrokBot { root, agents } => {
                    for root in [root, agents].into_iter().flatten() {
                        self.scan_session_dirs(HarnessId::GrokBot, &root, &[], true);
                    }
                }
                Probe::Db { harness, db } => {
                    if let Err(error) = self.scan_db(harness, &db) {
                        // A broken database store must not take down every
                        // query; its existing records stay until it recovers.
                        eprintln!("agent-transcript: {harness} discovery failed: {error}");
                    }
                }
            }
        }
        // Sources whose stores disappeared entirely: drop their records for
        // any harness that no probe covers anymore.
        self.drop_orphan_harnesses();
        self.resolve_pending();
        Ok(())
    }

    fn scan_files(&mut self, harness: HarnessId, root: &Path, rule: FileRule) {
        let rows = sources::walk_tree(root);
        let mut seen = HashSet::new();
        let root_str = root.to_string_lossy().into_owned();
        for (index, row) in rows.iter().enumerate() {
            if !rule.matches(&row.rel) {
                continue;
            }
            let source = root.join(&row.rel).to_string_lossy().into_owned();
            let fingerprint = file_fingerprint(index, &rows, rule);
            let updated = DateTime::<Utc>::from(
                std::time::UNIX_EPOCH + std::time::Duration::from_nanos(row.mtime_ns as u64),
            );
            seen.insert(source_key(harness, &source));
            self.reconsider(harness, &source, fingerprint, Some(updated));
        }
        self.drop_unseen(harness, Some(&root_str), &seen);
    }

    fn scan_session_dirs(
        &mut self,
        harness: HarnessId,
        root: &Path,
        markers: &[&str],
        grok_bot_rules: bool,
    ) {
        let rows = sources::walk_tree(root);
        let mut dirs = BTreeSet::new();
        for row in &rows {
            let Some((parent, name)) = row.rel.rsplit_once('/') else {
                continue;
            };
            let parent_name = parent.rsplit('/').next().unwrap_or(parent);
            let is_marker = markers.contains(&name)
                || (grok_bot_rules
                    && (name == "profile.json"
                        || name
                            .strip_suffix(".jsonl")
                            .is_some_and(|stem| stem == parent_name)));
            if is_marker {
                dirs.insert(parent.to_string());
            }
        }
        let mut seen = HashSet::new();
        let root_str = root.to_string_lossy().into_owned();
        for dir in dirs {
            let prefix = format!("{dir}/");
            let members: Vec<&TreeRow> = rows
                .iter()
                .filter(|row| row.rel.starts_with(&prefix))
                .collect();
            let fingerprint = sources::dir_fingerprint(members.iter().copied());
            let updated = members.iter().map(|row| row.mtime_ns).max().map(|ns| {
                DateTime::<Utc>::from(
                    std::time::UNIX_EPOCH + std::time::Duration::from_nanos(ns as u64),
                )
            });
            let source = root.join(&dir).to_string_lossy().into_owned();
            seen.insert(source_key(harness, &source));
            self.reconsider(harness, &source, fingerprint, updated);
        }
        self.drop_unseen(harness, Some(&root_str), &seen);
    }

    fn scan_db(&mut self, harness: HarnessId, db: &Path) -> Result<()> {
        let fingerprint = sources::db_fingerprint(db);
        if self.state.db_fingerprints.get(harness.as_str()) == Some(&fingerprint) {
            return Ok(());
        }
        let candidates = sources::discover_db_candidates(harness)?;
        let mut seen = HashSet::new();
        for candidate in candidates {
            seen.insert(source_key(harness, &candidate.source));
            self.reconsider(harness, &candidate.source, candidate.fingerprint, None);
        }
        self.drop_unseen(harness, None, &seen);
        self.state
            .db_fingerprints
            .insert(harness.as_str().to_string(), fingerprint);
        self.dirty = true;
        Ok(())
    }

    /// Load or reload one source. Called for new files, changed
    /// fingerprints, and pending records whose repository became resolvable.
    fn reconsider(
        &mut self,
        harness: HarnessId,
        source: &str,
        fingerprint: String,
        updated_at: Option<DateTime<Utc>>,
    ) {
        let key = source_key(harness, source);
        if let Some(record) = self.state.sources.get(&key) {
            if record.fingerprint == fingerprint && record.state != RecordState::Pending {
                return;
            }
            if record.fingerprint == fingerprint && record.state == RecordState::Pending {
                // Same body; only the repo resolution may have improved.
                if record.repo_key.is_some() || !self.cwd_resolves(record.cwd.as_deref()) {
                    return;
                }
            }
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
                let last_message_at = transcript.body.last().map(|message| message.timestamp);
                let message_count = transcript.body.len() as u64;
                let title = transcript.meta.title.clone();
                let git_branch = transcript.meta.git_branch.clone();
                let model = transcript.meta.model.clone();
                let (repo_key, content_hash, mut state) = match self.resolve_repo(cwd.as_deref()) {
                    SessionRepo::Key(repo_key) => {
                        let document = ArchiveDocument::new(harness, repo_key.clone(), transcript);
                        (
                            Some(repo_key),
                            document.content_hash().unwrap_or_default(),
                            RecordState::Ready,
                        )
                    }
                    SessionRepo::MissingCwd | SessionRepo::Unresolved(_) => {
                        (None, String::new(), RecordState::Pending)
                    }
                };
                if session_id.is_empty() {
                    state = RecordState::Broken;
                    self.report.broken += 1;
                }
                let record = SourceRecord {
                    harness,
                    source: source.to_string(),
                    fingerprint,
                    session_id,
                    repo_key,
                    state,
                    started_at,
                    updated_at: updated_at.or(loaded.updated_at),
                    last_message_at,
                    message_count,
                    content_hash,
                    title,
                    cwd,
                    git_branch,
                    model,
                };
                self.state.sources.insert(key, record);
                self.report.loaded += 1;
                self.dirty = true;
            }
            Err(LoadFailure::Transient(_)) => {
                // Keep the previous record — the file is probably locked or
                // half-written; the next diff retries it.
                self.report.transient += 1;
            }
            Err(LoadFailure::Broken(_)) => {
                self.state.sources.insert(
                    key,
                    SourceRecord {
                        harness,
                        source: source.to_string(),
                        fingerprint,
                        session_id: String::new(),
                        repo_key: None,
                        state: RecordState::Broken,
                        started_at: Utc::now(),
                        updated_at,
                        last_message_at: None,
                        message_count: 0,
                        content_hash: String::new(),
                        title: None,
                        cwd: None,
                        git_branch: None,
                        model: None,
                    },
                );
                self.report.broken += 1;
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

    fn cwd_resolves(&self, cwd: Option<&str>) -> bool {
        cwd.is_some_and(|cwd| {
            self.state
                .repos
                .get(cwd)
                .is_some_and(|entry| entry.repo_key.is_some())
        })
    }

    /// Records whose source vanished under this probe's root are gone.
    fn drop_unseen(&mut self, harness: HarnessId, root: Option<&str>, seen: &HashSet<String>) {
        let prefix = format!("{}\n", harness.as_str());
        let before = self.state.sources.len();
        self.state.sources.retain(|key, _| {
            let Some(rest) = key.strip_prefix(&prefix) else {
                return true;
            };
            if let Some(root) = root {
                if !rest.starts_with(root) {
                    return true;
                }
            }
            seen.contains(key)
        });
        let removed = before - self.state.sources.len();
        if removed > 0 {
            self.report.removed += removed;
            self.dirty = true;
        }
    }

    /// When a store root disappears entirely (harness uninstalled), its
    /// records are dropped on the next diff — sessions are files, so a
    /// missing store means missing sessions.
    fn drop_orphan_harnesses(&mut self) {
        let covered: HashSet<String> = probes()
            .iter()
            .map(|probe| probe.harness().as_str().to_string())
            .collect();
        let before = self.state.sources.len();
        self.state.sources.retain(|key, _| {
            let Some((harness, _)) = key.split_once('\n') else {
                return false;
            };
            covered.contains(harness)
        });
        let removed = before - self.state.sources.len();
        if removed > 0 {
            self.report.removed += removed;
            self.dirty = true;
        }
    }

    /// Pending records whose cwd now resolves get one body reload to compute
    /// their content hash.
    fn resolve_pending(&mut self) {
        let pending: Vec<(HarnessId, String, String, Option<DateTime<Utc>>)> = self
            .state
            .sources
            .values()
            .filter_map(|record| {
                if record.state != RecordState::Pending || record.repo_key.is_some() {
                    return None;
                }
                self.cwd_resolves(record.cwd.as_deref()).then(|| {
                    (
                        record.harness,
                        record.source.clone(),
                        record.fingerprint.clone(),
                        record.updated_at,
                    )
                })
            })
            .collect();
        for (harness, source, fingerprint, updated_at) in pending {
            self.reload(harness, &source, fingerprint, updated_at);
        }
    }
}

fn source_key(harness: HarnessId, source: &str) -> String {
    format!("{}\n{source}", harness.as_str())
}

/// Which local files identify a session source for a file-backed store.
#[derive(Clone, Copy)]
enum FileRule {
    /// Any file with this extension is a candidate session.
    Ext(&'static str),
    /// `local_*.json` records; the same-stem directory (audit log, project
    /// transcript) belongs to the same session and joins the fingerprint.
    CoworkRecord,
}

impl FileRule {
    fn matches(&self, rel: &str) -> bool {
        let name = rel.rsplit('/').next().unwrap_or(rel);
        match self {
            Self::Ext(ext) => name
                .rsplit_once('.')
                .is_some_and(|(_, suffix)| suffix == *ext),
            Self::CoworkRecord => name.starts_with("local_") && name.ends_with(".json"),
        }
    }

    fn db_sidecars(&self) -> bool {
        matches!(self, Self::Ext("db"))
    }

    fn extent_dir(&self) -> bool {
        matches!(self, Self::CoworkRecord)
    }
}

/// Fingerprint of one session source within a walked tree.
fn file_fingerprint(index: usize, rows: &[TreeRow], rule: FileRule) -> String {
    let row = &rows[index];
    let mut fingerprint = format!("{}:{}", row.mtime_ns, row.size);
    if rule.db_sidecars() {
        for suffix in ["-wal", "-shm"] {
            let want = format!("{}{suffix}", row.rel);
            if let Some(sidecar) = rows.iter().find(|row| row.rel == want) {
                fingerprint.push_str(&format!("|{}:{}", sidecar.mtime_ns, sidecar.size));
            }
        }
    }
    if rule.extent_dir() {
        let dir = row.rel.strip_suffix(".json").unwrap_or(&row.rel);
        let prefix = format!("{dir}/");
        let hash =
            sources::dir_fingerprint(rows.iter().filter(|member| member.rel.starts_with(&prefix)));
        fingerprint.push_str(&format!("|{hash}"));
    }
    fingerprint
}

enum Probe {
    Files {
        harness: HarnessId,
        root: PathBuf,
        rule: FileRule,
    },
    SessionDirs {
        harness: HarnessId,
        root: PathBuf,
        markers: &'static [&'static str],
    },
    GrokBot {
        root: Option<PathBuf>,
        agents: Option<PathBuf>,
    },
    Db {
        harness: HarnessId,
        db: PathBuf,
    },
}

impl Probe {
    fn harness(&self) -> HarnessId {
        match self {
            Self::Files { harness, .. }
            | Self::SessionDirs { harness, .. }
            | Self::Db { harness, .. } => *harness,
            Self::GrokBot { .. } => HarnessId::GrokBot,
        }
    }
}

fn probes() -> Vec<Probe> {
    let mut probes = Vec::new();
    if let Some(store) = claude_code::ClaudeStore::default_root() {
        probes.push(Probe::Files {
            harness: HarnessId::ClaudeCode,
            root: store.root,
            rule: FileRule::Ext("jsonl"),
        });
    }
    if let Some(store) = codex::CodexStore::default_root() {
        probes.push(Probe::Files {
            harness: HarnessId::Codex,
            root: store.sessions_dir,
            rule: FileRule::Ext("jsonl"),
        });
    }
    if let Some(store) = pi::PiStore::default_root() {
        probes.push(Probe::Files {
            harness: HarnessId::Pi,
            root: store.sessions_dir,
            rule: FileRule::Ext("jsonl"),
        });
    }
    if let Some(store) = campfire::CampfireStore::default_root() {
        probes.push(Probe::Files {
            harness: HarnessId::Campfire,
            root: store.sessions_dir,
            rule: FileRule::Ext("jsonl"),
        });
    }
    if let Some(store) = cursor::CursorStore::default_root() {
        probes.push(Probe::Files {
            harness: HarnessId::Cursor,
            root: store.chats_dir,
            rule: FileRule::Ext("db"),
        });
    }
    if let Some(store) = amp::AmpStore::default_root() {
        probes.push(Probe::Files {
            harness: HarnessId::Amp,
            root: store.threads_dir,
            rule: FileRule::Ext("json"),
        });
    }
    for store in sources::antigravity_stores() {
        probes.push(Probe::Files {
            harness: HarnessId::Antigravity,
            root: store.root.join("conversations"),
            rule: FileRule::Ext("db"),
        });
    }
    if let Some(store) = cowork::CoworkStore::default_root() {
        probes.push(Probe::Files {
            harness: HarnessId::Cowork,
            root: store.root,
            rule: FileRule::CoworkRecord,
        });
    }
    if let Some(store) = grok::GrokStore::default_root() {
        probes.push(Probe::SessionDirs {
            harness: HarnessId::Grok,
            root: store.sessions_dir,
            markers: &["updates.jsonl", "chat_history.jsonl"],
        });
    }
    if let Some(store) = grok_bot::GrokBotStore::default_root() {
        probes.push(Probe::GrokBot {
            root: Some(store.root),
            agents: store.agents,
        });
    }
    if let Some(store) = fx::FxStore::default_root() {
        probes.push(Probe::SessionDirs {
            harness: HarnessId::Fx,
            root: store.sessions_dir,
            markers: &["events.jsonl"],
        });
    }
    if let Some(store) = hermes::HermesStore::default_root() {
        probes.push(Probe::Db {
            harness: HarnessId::Hermes,
            db: store.db_path,
        });
    }
    if let Some(store) = cursor_desktop::CursorDesktopStore::default_root() {
        probes.push(Probe::Db {
            harness: HarnessId::CursorDesktop,
            db: store.user_dir.join("globalStorage").join("state.vscdb"),
        });
    }
    if let Some(store) = opencode::OpenCodeStore::default_db() {
        probes.push(Probe::Db {
            harness: HarnessId::OpenCode,
            db: store.db_path,
        });
    }
    probes
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
    fn fingerprints_cover_db_sidecars_and_cowork_extent() {
        let rows = vec![
            TreeRow {
                rel: "s/store.db".into(),
                mtime_ns: 1,
                size: 10,
            },
            TreeRow {
                rel: "s/store.db-wal".into(),
                mtime_ns: 2,
                size: 4,
            },
        ];
        let with_wal = file_fingerprint(0, &rows, FileRule::Ext("db"));
        let rows_without = &rows[..1];
        let without_wal = file_fingerprint(0, rows_without, FileRule::Ext("db"));
        assert_ne!(with_wal, without_wal);
    }

    #[test]
    fn session_dir_markers_detect_grok_and_fx() {
        // Marker-file detection runs in scan_session_dirs; this exercises the
        // pure rule: file named events.jsonl makes its parent a session dir.
        let rows = vec![
            TreeRow {
                rel: "abc/events.jsonl".into(),
                mtime_ns: 1,
                size: 5,
            },
            TreeRow {
                rel: "abc/session.json".into(),
                mtime_ns: 1,
                size: 5,
            },
            TreeRow {
                rel: "other/readme.txt".into(),
                mtime_ns: 1,
                size: 5,
            },
        ];
        let mut dirs = BTreeSet::new();
        for row in &rows {
            if let Some((parent, name)) = row.rel.rsplit_once('/') {
                if ["updates.jsonl", "chat_history.jsonl", "events.jsonl"].contains(&name) {
                    dirs.insert(parent.to_string());
                }
            }
        }
        assert_eq!(dirs.iter().next().map(String::as_str), Some("abc"));
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
                started_at: Utc::now(),
                updated_at: None,
                last_message_at: None,
                message_count: 3,
                content_hash: "abc".into(),
                title: None,
                cwd: None,
                git_branch: None,
                model: None,
            },
        );
        let bytes = serde_json::to_vec(&state).unwrap();
        let back: State = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.sources.len(), 1);
    }
}
