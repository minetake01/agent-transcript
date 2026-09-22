use std::collections::BTreeMap;
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

pub struct Collected {
    pub candidates: Vec<Candidate>,
    pub quiet: Vec<HarnessId>,
    pub generations: BTreeMap<String, String>,
}

pub struct Loaded {
    pub transcript: Transcript<Common>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub enum LoadFailure {
    Transient(String),
    Broken(String),
}

pub fn collect(stored_generations: &BTreeMap<String, String>) -> Result<Collected> {
    let mut collected = Collected {
        candidates: Vec::new(),
        quiet: Vec::new(),
        generations: BTreeMap::new(),
    };
    collect_files(&mut collected.candidates);
    collect_databases(stored_generations, &mut collected)?;
    Ok(collected)
}

pub fn load(candidate: &Candidate) -> std::result::Result<Loaded, LoadFailure> {
    let transcript = read_transcript(candidate)?;
    let updated_at = match candidate.harness {
        HarnessId::Hermes | HarnessId::CursorDesktop | HarnessId::OpenCode => None,
        _ => file_mtime(Path::new(&candidate.source)),
    };
    Ok(Loaded {
        transcript,
        updated_at,
    })
}

pub fn origin_fingerprint(cwd: &str) -> String {
    if cwd.is_empty() {
        return "missing-cwd".into();
    }
    let path = Path::new(cwd);
    if !path.is_dir() {
        return "missing-cwd".into();
    }
    let git = path.join(".git");
    if git.is_file() {
        return file_fingerprint(&git);
    }
    file_fingerprint(&git.join("config"))
}

fn collect_files(out: &mut Vec<Candidate>) {
    if let Some(store) = claude_code::ClaudeStore::default_root() {
        let mut files = Vec::new();
        walk_jsonl(&store.root, &["subagents", "tool-results"], &mut files);
        push_files(HarnessId::ClaudeCode, &files, out);
    }
    if let Some(store) = codex::CodexStore::default_root() {
        let mut files = Vec::new();
        walk_named(&store.sessions_dir, &mut files, |name, path| {
            name.starts_with("rollout-") && path.extension().is_some_and(|ext| ext == "jsonl")
        });
        push_files(HarnessId::Codex, &files, out);
    }
    if let Some(store) = pi::PiStore::default_root() {
        let mut files = Vec::new();
        walk_jsonl(&store.sessions_dir, &[], &mut files);
        push_files(HarnessId::Pi, &files, out);
    }
    if let Some(store) = campfire::CampfireStore::default_root() {
        let mut files = Vec::new();
        walk_jsonl(&store.sessions_dir, &[], &mut files);
        push_files(HarnessId::Campfire, &files, out);
    }
    if let Some(store) = cursor::CursorStore::default_root() {
        collect_cursor_stores(&store.chats_dir, out);
    }
    if let Some(store) = amp::AmpStore::default_root() {
        let mut files = Vec::new();
        if let Ok(entries) = fs::read_dir(&store.threads_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if entry.file_type().is_ok_and(|kind| kind.is_file())
                    && path.extension().is_some_and(|ext| ext == "json")
                {
                    files.push(path);
                }
            }
        }
        push_files(HarnessId::Amp, &files, out);
    }
    if let Some(store) = antigravity::AntigravityStore::default_root() {
        let mut files = Vec::new();
        if let Ok(entries) = fs::read_dir(store.root.join("conversations")) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "db") {
                    files.push(path);
                }
            }
        }
        push_files(HarnessId::Antigravity, &files, out);
    }
}

fn collect_databases(
    stored_generations: &BTreeMap<String, String>,
    collected: &mut Collected,
) -> Result<()> {
    if let Some(store) = grok::GrokStore::default_root() {
        gate_paths(
            HarnessId::Grok,
            "grok",
            tree_generation(&store.sessions_dir),
            Some(store),
            stored_generations,
            collected,
        )?;
    }
    if let Some(store) = grok_bot::GrokBotStore::default_root() {
        let generation = format!(
            "{}\n{}",
            tree_generation(&store.root),
            store
                .agents
                .as_ref()
                .map(|path| tree_generation(path))
                .unwrap_or_else(|| "missing".into())
        );
        gate_paths(
            HarnessId::GrokBot,
            "grok_bot",
            generation,
            Some(store),
            stored_generations,
            collected,
        )?;
    }
    if let Some(store) = fx::FxStore::default_root() {
        gate_paths(
            HarnessId::Fx,
            "fx",
            tree_generation(&store.sessions_dir),
            Some(store),
            stored_generations,
            collected,
        )?;
    }
    if let Some(store) = cowork::CoworkStore::default_root() {
        gate_paths(
            HarnessId::Cowork,
            "cowork",
            tree_generation(&store.root),
            Some(store),
            stored_generations,
            collected,
        )?;
    }
    if let Some(store) = hermes::HermesStore::default_root() {
        gate_ids(
            HarnessId::Hermes,
            "hermes",
            db_fingerprint(&store.db_path),
            Some(store),
            stored_generations,
            collected,
        )?;
    }
    if let Some(store) = cursor_desktop::CursorDesktopStore::default_root() {
        let db = store.user_dir.join("globalStorage").join("state.vscdb");
        gate_ids(
            HarnessId::CursorDesktop,
            "cursor_desktop",
            db_fingerprint(&db),
            Some(store),
            stored_generations,
            collected,
        )?;
    }
    if let Some(store) = opencode::OpenCodeStore::default_db() {
        gate_ids(
            HarnessId::OpenCode,
            "opencode",
            db_fingerprint(&store.db_path),
            Some(store),
            stored_generations,
            collected,
        )?;
    }
    Ok(())
}

fn gate_paths<S>(
    harness: HarnessId,
    key: &str,
    generation: String,
    store: Option<S>,
    stored_generations: &BTreeMap<String, String>,
    collected: &mut Collected,
) -> Result<()>
where
    S: Store<Ref = PathBuf>,
{
    if stored_generations.get(key) == Some(&generation) {
        collected.quiet.push(harness);
        return Ok(());
    }
    let Some(store) = store else {
        collected.generations.insert(key.to_string(), generation);
        return Ok(());
    };
    let discovered = store.discover().map_err(tx_error)?;
    let refs: Vec<PathBuf> = discovered.into_iter().map(|item| item.reference).collect();
    let fingerprints = store.fingerprints(&refs).unwrap_or_default();
    for path in refs {
        let source = path.to_string_lossy().into_owned();
        let fingerprint = fingerprints
            .get(&source)
            .cloned()
            .filter(|fingerprint| !fingerprint.is_empty())
            .unwrap_or_else(|| file_fingerprint(&path));
        collected.candidates.push(Candidate {
            harness,
            source,
            fingerprint,
        });
    }
    collected.generations.insert(key.to_string(), generation);
    Ok(())
}

fn gate_ids<S>(
    harness: HarnessId,
    key: &str,
    generation: String,
    store: Option<S>,
    stored_generations: &BTreeMap<String, String>,
    collected: &mut Collected,
) -> Result<()>
where
    S: Store<Ref = String>,
{
    if stored_generations.get(key) == Some(&generation) {
        collected.quiet.push(harness);
        return Ok(());
    }
    let Some(store) = store else {
        collected.generations.insert(key.to_string(), generation);
        return Ok(());
    };
    let discovered = store.discover().map_err(tx_error)?;
    let refs: Vec<String> = discovered.into_iter().map(|item| item.reference).collect();
    let fingerprints = store.fingerprints(&refs).unwrap_or_default();
    for id in refs {
        let fingerprint = fingerprints.get(&id).cloned().unwrap_or_default();
        collected.candidates.push(Candidate {
            harness,
            source: id,
            fingerprint,
        });
    }
    collected.generations.insert(key.to_string(), generation);
    Ok(())
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
            load_path(antigravity::AntigravityStore::default_root(), candidate)
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
    error.raw_os_error() == Some(32)
        || matches!(
            error.kind(),
            ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
        )
}

fn tx_error(error: txcript::Error) -> Error {
    Error::msg(error.to_string())
}

fn push_files(harness: HarnessId, files: &[PathBuf], out: &mut Vec<Candidate>) {
    for path in files {
        out.push(Candidate {
            harness,
            source: path.to_string_lossy().into_owned(),
            fingerprint: file_fingerprint(path),
        });
    }
}

fn collect_cursor_stores(chats_dir: &Path, out: &mut Vec<Candidate>) {
    let Ok(workspaces) = fs::read_dir(chats_dir) else {
        return;
    };
    for workspace in workspaces.flatten() {
        if !workspace.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Ok(sessions) = fs::read_dir(workspace.path()) else {
            continue;
        };
        for session in sessions.flatten() {
            let db = session.path().join("store.db");
            if db.is_file() {
                push_files(HarnessId::Cursor, &[db], out);
            }
        }
    }
}

fn walk_named(dir: &Path, out: &mut Vec<PathBuf>, include: impl Fn(&str, &Path) -> bool + Copy) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            walk_named(&path, out, include);
            continue;
        }
        if include(&name, &path) {
            out.push(path);
        }
    }
}

fn walk_jsonl(dir: &Path, skip_dirs: &[&str], out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            if !skip_dirs.contains(&name.as_ref()) {
                walk_jsonl(&path, skip_dirs, out);
            }
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "jsonl") {
            out.push(path);
        }
    }
}

fn file_fingerprint(path: &Path) -> String {
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

fn file_mtime(path: &Path) -> Option<DateTime<Utc>> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    Some(DateTime::<Utc>::from(modified))
}

fn db_fingerprint(path: &Path) -> String {
    [path, &sidecar(path, "-wal"), &sidecar(path, "-shm")]
        .into_iter()
        .map(file_fingerprint)
        .collect::<Vec<_>>()
        .join("|")
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|file_name| file_name.to_os_string())
        .unwrap_or_default();
    name.push(suffix);
    path.with_file_name(name)
}

fn tree_generation(root: &Path) -> String {
    if !root.exists() {
        return "missing".into();
    }
    let mut rows = Vec::new();
    walk_rows(root, root, &mut rows);
    rows.sort();
    let mut hasher = Sha256::new();
    for row in &rows {
        hasher.update(row.as_bytes());
        hasher.update([b'\n']);
    }
    hex::encode(hasher.finalize())
}

fn walk_rows(root: &Path, dir: &Path, rows: &mut Vec<String>) {
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
        rows.push(format!("{}:{modified}:{}", relative.display(), meta.len()));
    }
}
