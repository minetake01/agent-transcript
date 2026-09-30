//! The harness registry: which stores exist on this machine, what identifies
//! a session source inside each, and how a source's transcript loads.
//!
//! `sites()` is the single table of local archive locations. The persistent
//! source state (`local_state`) walks every site's rows and applies its
//! `Kind` rules to enumerate sources; `load` turns a source back into a
//! transcript. Ingest and the MCP request path both read the records that
//! diff produces, so this table is the only place that knows where
//! transcripts live.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use txcript::harness::{
    amp, antigravity, campfire, claude_code, codex, cowork, cursor, cursor_desktop, fx, grok,
    grok_bot, hermes, opencode, pi,
};
use txcript::Common;
use txcript::{Codec, HarnessId, Store, Transcript};

use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Candidate {
    pub harness: HarnessId,
    pub source: String,
    pub fingerprint: String,
}

pub struct Loaded {
    pub transcript: Transcript<Common>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub enum LoadFailure {
    Transient(String),
    Broken(String),
}

/// One local archive location: where its session sources live and how a
/// source is identified in the file listing.
pub struct Site {
    pub harness: HarnessId,
    /// Directory walked for file- and directory-based sources; the database
    /// file itself for [`Kind::Db`].
    pub path: PathBuf,
    pub kind: Kind,
}

/// How a session source is identified inside a site's file listing.
pub enum Kind {
    /// Each file matching `rule` is one session. Paths with a component in
    /// `exclude_dirs` are not sessions (Claude Code's `subagents` and
    /// `tool-results` directories hold side files, not transcripts).
    Files {
        rule: FileRule,
        exclude_dirs: &'static [&'static str],
    },
    /// A directory containing one of `markers` is one session.
    SessionDirs { markers: &'static [&'static str] },
    /// A directory holding `profile.json` or a `.jsonl` named after itself
    /// is one Grok Bot session.
    GrokBotDirs,
    /// Sessions live inside the database at `path`; enumerated through
    /// txcript only when the database fingerprint moves.
    Db,
}

/// Which local files identify a session source for a file-backed store.
#[derive(Clone, Copy)]
pub enum FileRule {
    /// Any file with this extension is a candidate session.
    Ext(&'static str),
    /// `local_*.json` records; the same-stem directory (audit log, project
    /// transcript) belongs to the same session and joins the fingerprint.
    CoworkRecord,
}

impl FileRule {
    pub(crate) fn matches(&self, rel: &str) -> bool {
        let name = rel.rsplit('/').next().unwrap_or(rel);
        match self {
            Self::Ext(ext) => name
                .rsplit_once('.')
                .is_some_and(|(_, suffix)| suffix == *ext),
            Self::CoworkRecord => name.starts_with("local_") && name.ends_with(".json"),
        }
    }

    /// SQLite databases share their write state with `-wal`/`-shm` sidecars;
    /// their stats belong to the session's fingerprint.
    fn db_sidecars(&self) -> bool {
        matches!(self, Self::Ext("db"))
    }

    /// Cowork records own a same-stem directory of session files.
    fn extent_dir(&self) -> bool {
        matches!(self, Self::CoworkRecord)
    }
}

impl Kind {
    /// Whether `rel` is a session file under the site root.
    pub(crate) fn matches_file(&self, rel: &str) -> bool {
        match self {
            Self::Files { rule, exclude_dirs } => {
                rule.matches(rel) && !rel.split('/').any(|part| exclude_dirs.contains(&part))
            }
            _ => false,
        }
    }

    /// Whether `rel` — a file at `parent/name` under the site root — marks
    /// `parent` as a session directory.
    pub(crate) fn is_dir_marker(&self, parent: &str, name: &str) -> bool {
        match self {
            Self::SessionDirs { markers } => markers.contains(&name),
            Self::GrokBotDirs => {
                let parent_name = parent.rsplit('/').next().unwrap_or(parent);
                name == "profile.json"
                    || name
                        .strip_suffix(".jsonl")
                        .is_some_and(|stem| stem == parent_name)
            }
            _ => false,
        }
    }
}

/// Every session store present on this machine.
pub fn sites() -> Vec<Site> {
    let mut sites = Vec::new();
    if let Some(store) = claude_code::ClaudeStore::default_root() {
        sites.push(Site {
            harness: HarnessId::ClaudeCode,
            path: store.root,
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &["subagents", "tool-results"],
            },
        });
    }
    if let Some(store) = codex::CodexStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Codex,
            path: store.sessions_dir,
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &[],
            },
        });
    }
    if let Some(store) = pi::PiStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Pi,
            path: store.sessions_dir,
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &[],
            },
        });
    }
    if let Some(store) = campfire::CampfireStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Campfire,
            path: store.sessions_dir,
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &[],
            },
        });
    }
    if let Some(store) = cursor::CursorStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Cursor,
            path: store.chats_dir,
            kind: Kind::Files {
                rule: FileRule::Ext("db"),
                exclude_dirs: &[],
            },
        });
    }
    if let Some(store) = amp::AmpStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Amp,
            path: store.threads_dir,
            kind: Kind::Files {
                rule: FileRule::Ext("json"),
                exclude_dirs: &[],
            },
        });
    }
    for store in antigravity_stores() {
        sites.push(Site {
            harness: HarnessId::Antigravity,
            path: store.root.join("conversations"),
            kind: Kind::Files {
                rule: FileRule::Ext("db"),
                exclude_dirs: &[],
            },
        });
    }
    if let Some(store) = cowork::CoworkStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Cowork,
            path: store.root,
            kind: Kind::Files {
                rule: FileRule::CoworkRecord,
                exclude_dirs: &[],
            },
        });
    }
    if let Some(store) = grok::GrokStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Grok,
            path: store.sessions_dir,
            kind: Kind::SessionDirs {
                markers: &["updates.jsonl", "chat_history.jsonl"],
            },
        });
    }
    if let Some(store) = grok_bot::GrokBotStore::default_root() {
        sites.push(Site {
            harness: HarnessId::GrokBot,
            path: store.root,
            kind: Kind::GrokBotDirs,
        });
        if let Some(agents) = store.agents {
            sites.push(Site {
                harness: HarnessId::GrokBot,
                path: agents,
                kind: Kind::GrokBotDirs,
            });
        }
    }
    if let Some(store) = fx::FxStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Fx,
            path: store.sessions_dir,
            kind: Kind::SessionDirs {
                markers: &["events.jsonl"],
            },
        });
    }
    if let Some(store) = hermes::HermesStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Hermes,
            path: store.db_path,
            kind: Kind::Db,
        });
    }
    if let Some(store) = cursor_desktop::CursorDesktopStore::default_root() {
        sites.push(Site {
            harness: HarnessId::CursorDesktop,
            path: store.user_dir.join("globalStorage").join("state.vscdb"),
            kind: Kind::Db,
        });
    }
    if let Some(store) = opencode::OpenCodeStore::default_db() {
        sites.push(Site {
            harness: HarnessId::OpenCode,
            path: store.db_path,
            kind: Kind::Db,
        });
    }
    sites
}

pub fn load(candidate: &Candidate) -> std::result::Result<Loaded, LoadFailure> {
    let transcript = read_transcript(candidate)?;
    // Database-backed stores have no meaningful source mtime; directory
    // sources report their newest member's mtime so freshness agrees with
    // the scanned record.
    let updated_at = match candidate.harness {
        HarnessId::Hermes | HarnessId::CursorDesktop | HarnessId::OpenCode => None,
        _ => source_mtime(Path::new(&candidate.source)),
    };
    Ok(Loaded {
        transcript,
        updated_at,
    })
}

fn source_mtime(path: &Path) -> Option<DateTime<Utc>> {
    if path.is_dir() {
        walk_tree(path)
            .iter()
            .map(|row| row.mtime_ns)
            .max()
            .map(mtime_datetime)
    } else {
        file_mtime(path)
    }
}

/// Enumerate sessions of a database-backed store. Used by the source-state
/// diff, which calls this only when the database fingerprint changed.
pub fn discover_db_candidates(harness: HarnessId) -> Result<Vec<Candidate>> {
    match harness {
        HarnessId::Hermes => {
            let store = hermes::HermesStore::default_root()
                .ok_or_else(|| Error::msg("hermes store is unavailable"))?;
            enumerate(harness, store)
        }
        HarnessId::CursorDesktop => {
            let store = cursor_desktop::CursorDesktopStore::default_root()
                .ok_or_else(|| Error::msg("cursor desktop store is unavailable"))?;
            enumerate(harness, store)
        }
        HarnessId::OpenCode => {
            let store = opencode::OpenCodeStore::default_db()
                .ok_or_else(|| Error::msg("opencode store is unavailable"))?;
            enumerate(harness, store)
        }
        _ => Err(Error::msg(format!(
            "{harness} is not a database-backed store"
        ))),
    }
}

fn enumerate<S>(harness: HarnessId, store: S) -> Result<Vec<Candidate>>
where
    S: Store<Ref = String>,
{
    let discovered = store.discover().map_err(tx_error)?;
    let refs: Vec<String> = discovered
        .iter()
        .map(|item| item.reference.clone())
        .collect();
    let fingerprints = store.fingerprints(&refs).map_err(tx_error)?;
    let mut candidates = Vec::with_capacity(discovered.len());
    for item in &discovered {
        let source = item.reference.clone();
        let fingerprint = fingerprints.get(&source).cloned().unwrap_or_default();
        candidates.push(Candidate {
            harness,
            source,
            fingerprint,
        });
    }
    Ok(candidates)
}

fn read_transcript(candidate: &Candidate) -> std::result::Result<Transcript<Common>, LoadFailure> {
    match candidate.harness {
        HarnessId::ClaudeCode => load_path(claude_code::ClaudeStore::default_root(), candidate),
        HarnessId::Codex => load_path(codex::CodexStore::default_root(), candidate),
        HarnessId::Pi => load_path(pi::PiStore::default_root(), candidate),
        HarnessId::Campfire => load_path(campfire::CampfireStore::default_root(), candidate),
        HarnessId::Cursor => load_path(cursor::CursorStore::default_root(), candidate),
        HarnessId::Amp => load_path(amp::AmpStore::default_root(), candidate),
        HarnessId::Antigravity => {
            let path = PathBuf::from(&candidate.source);
            let root = path
                .parent()
                .and_then(|p| p.parent())
                .map(PathBuf::from)
                .or_else(|| antigravity::AntigravityStore::default_root().map(|s| s.root));
            let store = root.map(antigravity::AntigravityStore::new);
            load_path(store, candidate)
        }
        HarnessId::Grok => load_path(grok::GrokStore::default_root(), candidate),
        HarnessId::GrokBot => load_path(grok_bot::GrokBotStore::default_root(), candidate),
        HarnessId::Fx => load_path(fx::FxStore::default_root(), candidate),
        HarnessId::Cowork => load_path(cowork::CoworkStore::default_root(), candidate),
        HarnessId::Hermes => load_id(hermes::HermesStore::default_root(), candidate),
        HarnessId::CursorDesktop => load_id(
            cursor_desktop::CursorDesktopStore::default_root(),
            candidate,
        ),
        HarnessId::OpenCode => load_id(opencode::OpenCodeStore::default_db(), candidate),
        HarnessId::ClaudeChat | HarnessId::ChatGpt | HarnessId::Simple => Err(LoadFailure::Broken(
            format!("{} is not a local archive source", candidate.harness),
        )),
    }
}

fn load_path<S>(
    store: Option<S>,
    candidate: &Candidate,
) -> std::result::Result<Transcript<Common>, LoadFailure>
where
    S: Store<Ref = PathBuf>,
    S::H: Codec,
{
    let store = store.ok_or_else(|| {
        LoadFailure::Broken(format!("{} store is unavailable", candidate.harness))
    })?;
    let path = PathBuf::from(&candidate.source);
    let native = store.load(&path).map_err(load_failure)?;
    <S::H as Codec>::to_common(&native).map_err(load_failure)
}

fn load_id<S>(
    store: Option<S>,
    candidate: &Candidate,
) -> std::result::Result<Transcript<Common>, LoadFailure>
where
    S: Store<Ref = String>,
    S::H: Codec,
{
    let store = store.ok_or_else(|| {
        LoadFailure::Broken(format!("{} store is unavailable", candidate.harness))
    })?;
    let native = store.load(&candidate.source).map_err(load_failure)?;
    <S::H as Codec>::to_common(&native).map_err(load_failure)
}

fn load_failure(error: txcript::Error) -> LoadFailure {
    match &error {
        txcript::Error::Io(io) if is_transient_io(io) => LoadFailure::Transient(error.to_string()),
        txcript::Error::Remote { .. } => LoadFailure::Transient(error.to_string()),
        _ => LoadFailure::Broken(error.to_string()),
    }
}

fn is_transient_io(error: &std::io::Error) -> bool {
    // 32 is ERROR_SHARING_VIOLATION — Windows-only; on unix the same raw
    // code means EPIPE, which is not a retryable source-read failure.
    #[cfg(windows)]
    if error.raw_os_error() == Some(32) {
        return true;
    }
    matches!(
        error.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    )
}

fn tx_error(error: txcript::Error) -> Error {
    Error::msg(error.to_string())
}

fn file_mtime(path: &Path) -> Option<DateTime<Utc>> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    Some(DateTime::<Utc>::from(modified))
}

pub(crate) fn mtime_datetime(mtime_ns: u128) -> DateTime<Utc> {
    DateTime::<Utc>::from(
        UNIX_EPOCH + std::time::Duration::from_nanos(u64::try_from(mtime_ns).unwrap_or(u64::MAX)),
    )
}

pub(crate) fn db_fingerprint(path: &Path) -> String {
    [path, &sidecar(path, "-wal"), &sidecar(path, "-shm")]
        .into_iter()
        .map(stat_fingerprint)
        .collect::<Vec<_>>()
        .join("|")
}

pub(crate) fn stat_fingerprint(path: &Path) -> String {
    match fs::metadata(path) {
        Err(_) => String::new(),
        Ok(meta) => {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |duration| duration.as_nanos());
            format!("{mtime}:{}", meta.len())
        }
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|file_name| file_name.to_os_string())
        .unwrap_or_default();
    name.push(suffix);
    path.with_file_name(name)
}

/// Fingerprint of one file-based session source within a walked tree. The
/// site's rule decides which neighbouring rows join the fingerprint.
pub(crate) fn row_fingerprint(index: usize, rows: &[TreeRow], site: &Site) -> String {
    let row = &rows[index];
    let mut fingerprint = format!("{}:{}", row.mtime_ns, row.size);
    let Kind::Files { rule, .. } = &site.kind else {
        return fingerprint;
    };
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
        let hash = dir_fingerprint(rows.iter().filter(|member| member.rel.starts_with(&prefix)));
        fingerprint.push_str(&format!("|{hash}"));
    }
    fingerprint
}

/// One file under a store root, relative to the root.
pub(crate) struct TreeRow {
    pub rel: String,
    pub mtime_ns: u128,
    pub size: u64,
}

/// Stat-only recursive listing of a store root.
pub(crate) fn walk_tree(root: &Path) -> Vec<TreeRow> {
    let mut rows = Vec::new();
    if root.exists() {
        walk_rows(root, root, &mut rows);
    }
    rows.sort_by(|left, right| left.rel.cmp(&right.rel));
    rows
}

fn tree_rows_hash<'a>(rows: impl Iterator<Item = &'a TreeRow>) -> Vec<u8> {
    let mut hasher = Sha256::new();
    for row in rows {
        hasher.update(format!("{}:{}:{}", row.rel, row.mtime_ns, row.size).as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().to_vec()
}

/// Fingerprint every file under `dir` — for session sources that are a
/// directory (grok/fx/grok_bot session folders) rather than a single file.
pub(crate) fn dir_fingerprint<'a>(rows: impl Iterator<Item = &'a TreeRow>) -> String {
    hex::encode(tree_rows_hash(rows))
}

/// Join a `/`-separated relative path under `root` using OS separators, so
/// stored source strings stay canonical.
pub(crate) fn join_rel(root: &Path, rel: &str) -> PathBuf {
    root.join(rel.split('/').collect::<PathBuf>())
}

fn walk_rows(root: &Path, dir: &Path, rows: &mut Vec<TreeRow>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            walk_rows(root, &path, rows);
            continue;
        }
        let Ok(meta) = fs::metadata(&path) else {
            continue;
        };
        let modified = meta
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_nanos());
        let relative = path.strip_prefix(root).unwrap_or(&path);
        // Rows always use `/` separators so marker and exclusion matching is
        // platform-independent.
        let rel = relative
            .iter()
            .map(|component| component.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        rows.push(TreeRow {
            rel,
            mtime_ns: modified,
            size: meta.len(),
        });
    }
}

pub fn antigravity_stores() -> Vec<antigravity::AntigravityStore> {
    let mut stores = Vec::new();
    if let Some(store) = antigravity::AntigravityStore::default_root() {
        if store.root.join("conversations").is_dir() {
            stores.push(store);
        }
    }
    if let Some(home) = crate::config::user_home_dir() {
        let ide_root = home.join(".gemini").join("antigravity");
        if ide_root.join("conversations").is_dir() {
            let ide_store = antigravity::AntigravityStore::new(&ide_root);
            if !stores.iter().any(|s| s.root == ide_store.root) {
                stores.push(ide_store);
            }
        }
    }
    stores
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(kind: Kind) -> Site {
        Site {
            harness: HarnessId::Codex,
            path: PathBuf::from("/root"),
            kind,
        }
    }

    fn row(rel: &str, mtime_ns: u128, size: u64) -> TreeRow {
        TreeRow {
            rel: rel.into(),
            mtime_ns,
            size,
        }
    }

    #[test]
    fn file_sites_match_rules_and_skip_excluded_dirs() {
        let files = Kind::Files {
            rule: FileRule::Ext("jsonl"),
            exclude_dirs: &["subagents", "tool-results"],
        };
        assert!(files.matches_file("a/b/session.jsonl"));
        assert!(!files.matches_file("a/b/session.json"));
        assert!(!files.matches_file("a/subagents/agent-1.jsonl"));
        assert!(!files.matches_file("a/x/tool-results/t.jsonl"));
        let db = Kind::Files {
            rule: FileRule::Ext("db"),
            exclude_dirs: &[],
        };
        assert!(db.matches_file("chat/store.db"));
        assert!(!db.matches_file("chat/store.db-wal"));
    }

    #[test]
    fn dir_sites_match_markers() {
        let grok = Kind::SessionDirs {
            markers: &["updates.jsonl", "chat_history.jsonl"],
        };
        assert!(grok.is_dir_marker("s1", "updates.jsonl"));
        assert!(!grok.is_dir_marker("s1", "events.jsonl"));
        let grok_bot = Kind::GrokBotDirs;
        assert!(grok_bot.is_dir_marker("agents/bot-1", "profile.json"));
        assert!(grok_bot.is_dir_marker("sessions/abc", "abc.jsonl"));
        assert!(!grok_bot.is_dir_marker("sessions/abc", "other.jsonl"));
    }

    #[test]
    fn fingerprints_cover_db_sidecars_and_cowork_extent() {
        let site = site(Kind::Files {
            rule: FileRule::Ext("db"),
            exclude_dirs: &[],
        });
        let rows = vec![row("s/store.db", 1, 10), row("s/store.db-wal", 2, 4)];
        let with_wal = row_fingerprint(0, &rows, &site);
        let without_wal = row_fingerprint(0, &rows[..1], &site);
        assert_ne!(with_wal, without_wal);
    }
}
