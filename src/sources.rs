//! The harness registry: which stores exist on this machine, how a session
//! source is identified inside each, and how a source loads.
//!
//! `sites()` is the single table of local archive locations; each [`Site`]
//! carries the store that enumerates and loads its sources. `local_state`
//! walks every site's rows and applies its `Kind` rules; [`load`] finds the
//! site a source belongs to and reads it. Ingest and the MCP request path
//! both read the records the diff produces, so this table is the only place
//! that knows where transcripts live.

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

/// A session found inside a database-backed site: the locator string the
/// site writes into records and the store's own change fingerprint.
pub struct Discovery {
    pub source: String,
    pub fingerprint: String,
}

pub enum LoadFailure {
    Transient(String),
    Broken(String),
}

/// Whether this harness has a local archive `sites()` can describe. Claude
/// Chat and ChatGPT are live APIs; `Simple` is a txcript-internal baseline.
pub fn is_local(harness: HarnessId) -> bool {
    !matches!(
        harness,
        HarnessId::ClaudeChat | HarnessId::ChatGpt | HarnessId::Simple
    )
}

/// One local archive location: where its session sources live and how a
/// source is identified in the file listing.
pub struct Site {
    pub harness: HarnessId,
    /// Directory walked for file- and directory-based sources; the database
    /// file itself for [`Kind::Db`].
    pub path: PathBuf,
    pub kind: Kind,
    loader: Loader,
}

/// Type-erased access to the txcript store backing a [`Site`].
enum Loader {
    /// `Store<Ref = PathBuf>` — the source string is a filesystem path.
    Path(Box<dyn PathLoader>),
    /// `Store<Ref = String>` — sessions live inside the `path` database and
    /// sources are `"{db path}\n{reference}"`.
    Db(Box<dyn DbLoader>),
}

impl Site {
    /// The prefix every source under this site carries. For file- and
    /// directory-backed sites it is the site root itself; a database site
    /// prefixes its references with the database path and a newline.
    fn source_prefix(&self) -> String {
        match &self.loader {
            Loader::Path(_) => self.path.to_string_lossy().into_owned(),
            Loader::Db(_) => format!("{}\n", self.path.to_string_lossy()),
        }
    }

    /// Whether `source` belongs to this site. Path prefixes compare by
    /// component, so a sibling directory whose name shares a prefix does not
    /// match.
    pub fn owns(&self, source: &str) -> bool {
        match &self.loader {
            Loader::Path(_) => Path::new(source).starts_with(&self.path),
            Loader::Db(_) => source.starts_with(&self.source_prefix()),
        }
    }

    /// Load the transcript a source of this site locates.
    pub fn load(&self, source: &str) -> std::result::Result<Transcript<Common>, LoadFailure> {
        match &self.loader {
            Loader::Path(loader) => loader.load(Path::new(source)),
            Loader::Db(loader) => {
                let prefix = self.source_prefix();
                let Some(reference) = source.strip_prefix(&prefix) else {
                    return Err(LoadFailure::Broken(format!(
                        "source `{source}` is not in database {}",
                        self.path.display()
                    )));
                };
                loader.load(reference)
            }
        }
    }

    /// Enumerate the sessions of a database-backed site. Called by the
    /// source-state diff only when the database fingerprint moved.
    pub fn discover(&self) -> Result<Vec<Discovery>> {
        let Loader::Db(loader) = &self.loader else {
            return Err(Error::msg(format!(
                "{} is not a database-backed site",
                self.harness
            )));
        };
        let prefix = self.source_prefix();
        loader.discover().map(|found| {
            found
                .into_iter()
                .map(|(reference, fingerprint)| Discovery {
                    source: format!("{prefix}{reference}"),
                    fingerprint,
                })
                .collect()
        })
    }
}

/// Load the transcript a source locates. The site that produced the source
/// is found again by prefix — the longest matching site path wins — so
/// `local_state` records carry everything `load` needs.
pub fn load(
    harness: HarnessId,
    source: &str,
) -> std::result::Result<Transcript<Common>, LoadFailure> {
    sites()
        .into_iter()
        .filter(|site| site.harness == harness && site.owns(source))
        .max_by_key(|site| site.path.as_os_str().len())
        .ok_or_else(|| LoadFailure::Broken(format!("{harness} has no store for `{source}`")))?
        .load(source)
}

trait PathLoader {
    fn load(&self, path: &Path) -> std::result::Result<Transcript<Common>, LoadFailure>;
}

impl<S> PathLoader for S
where
    S: Store<Ref = PathBuf>,
    S::H: Codec,
{
    fn load(&self, path: &Path) -> std::result::Result<Transcript<Common>, LoadFailure> {
        let native = Store::load(self, &path.to_path_buf()).map_err(load_failure)?;
        <S::H as Codec>::to_common(&native).map_err(load_failure)
    }
}

trait DbLoader {
    /// `(reference, fingerprint)` pairs for every session in the database.
    fn discover(&self) -> Result<Vec<(String, String)>>;
    fn load(&self, reference: &str) -> std::result::Result<Transcript<Common>, LoadFailure>;
}

impl<S> DbLoader for S
where
    S: Store<Ref = String>,
    S::H: Codec,
{
    fn discover(&self) -> Result<Vec<(String, String)>> {
        let discovered = Store::discover(self).map_err(tx_error)?;
        let refs: Vec<String> = discovered
            .iter()
            .map(|item| item.reference.clone())
            .collect();
        let fingerprints = Store::fingerprints(self, &refs).map_err(tx_error)?;
        Ok(discovered
            .into_iter()
            .map(|item| {
                let fingerprint = fingerprints
                    .get(&item.reference)
                    .cloned()
                    .unwrap_or_default();
                (item.reference, fingerprint)
            })
            .collect())
    }

    fn load(&self, reference: &str) -> std::result::Result<Transcript<Common>, LoadFailure> {
        let native = Store::load(self, &reference.to_string()).map_err(load_failure)?;
        <S::H as Codec>::to_common(&native).map_err(load_failure)
    }
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
            path: store.root.clone(),
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &["subagents", "tool-results"],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = codex::CodexStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Codex,
            path: store.sessions_dir.clone(),
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &[],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = pi::PiStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Pi,
            path: store.sessions_dir.clone(),
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &[],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = campfire::CampfireStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Campfire,
            path: store.sessions_dir.clone(),
            kind: Kind::Files {
                rule: FileRule::Ext("jsonl"),
                exclude_dirs: &[],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = cursor::CursorStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Cursor,
            path: store.chats_dir.clone(),
            kind: Kind::Files {
                rule: FileRule::Ext("db"),
                exclude_dirs: &[],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = amp::AmpStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Amp,
            path: store.threads_dir.clone(),
            kind: Kind::Files {
                rule: FileRule::Ext("json"),
                exclude_dirs: &[],
            },
            loader: Loader::Path(Box::new(store)),
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
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = cowork::CoworkStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Cowork,
            path: store.root.clone(),
            kind: Kind::Files {
                rule: FileRule::CoworkRecord,
                exclude_dirs: &[],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = grok::GrokStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Grok,
            path: store.sessions_dir.clone(),
            kind: Kind::SessionDirs {
                markers: &["updates.jsonl", "chat_history.jsonl"],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = grok_bot::GrokBotStore::default_root() {
        let agents = store.agents.clone();
        sites.push(Site {
            harness: HarnessId::GrokBot,
            path: store.root.clone(),
            kind: Kind::GrokBotDirs,
            loader: Loader::Path(Box::new(store)),
        });
        if let Some(agents) = agents {
            // A store rooted anywhere loads by absolute path; the root only
            // decides what discovery walks.
            sites.push(Site {
                harness: HarnessId::GrokBot,
                path: agents.clone(),
                kind: Kind::GrokBotDirs,
                loader: Loader::Path(Box::new(grok_bot::GrokBotStore::new(agents))),
            });
        }
    }
    if let Some(store) = fx::FxStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Fx,
            path: store.sessions_dir.clone(),
            kind: Kind::SessionDirs {
                markers: &["events.jsonl"],
            },
            loader: Loader::Path(Box::new(store)),
        });
    }
    if let Some(store) = hermes::HermesStore::default_root() {
        sites.push(Site {
            harness: HarnessId::Hermes,
            path: store.db_path.clone(),
            kind: Kind::Db,
            loader: Loader::Db(Box::new(store)),
        });
    }
    if let Some(store) = cursor_desktop::CursorDesktopStore::default_root() {
        sites.push(Site {
            harness: HarnessId::CursorDesktop,
            path: store.user_dir.join("globalStorage").join("state.vscdb"),
            kind: Kind::Db,
            loader: Loader::Db(Box::new(store)),
        });
    }
    if let Some(store) = opencode::OpenCodeStore::default_db() {
        sites.push(Site {
            harness: HarnessId::OpenCode,
            path: store.db_path.clone(),
            kind: Kind::Db,
            loader: Loader::Db(Box::new(store)),
        });
    }
    sites
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
    match std::fs::metadata(path) {
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
    let Ok(entries) = std::fs::read_dir(dir) else {
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
        let Ok(meta) = std::fs::metadata(&path) else {
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
