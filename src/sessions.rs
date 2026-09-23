use std::collections::{HashMap, HashSet};
use std::path::Path;

use txcript::local::Session;
use txcript::{local, Common, HarnessId, Transcript};

use crate::catalog::Catalog;
use crate::crypto::Key;
use crate::document::ArchiveDocument;
use crate::error::{Error, Result};
use crate::merge::{
    choose_current, select, Current, Freshness, LocalView, MergedView, Pick, RemoteView,
};
use crate::remote::{document_from_plaintext, load_plaintext};
use crate::repo_id::{scope_directory, RepoCache, SessionRepo};
use crate::store::R2;

pub struct Scope {
    pub repo_key: String,
    pub merged: Vec<MergedView>,
    sessions: HashMap<(HarnessId, String), Session>,
    loaded: HashMap<(HarnessId, String), Transcript<Common>>,
    fingerprints: HashMap<(HarnessId, String), String>,
}

impl Scope {
    pub fn prepare_search_fingerprints(&mut self) {
        let keys: Vec<_> = self.sessions.keys().cloned().collect();
        let ordered: Vec<_> = keys
            .iter()
            .filter_map(|key| self.sessions.remove(key))
            .collect();
        let values = local::fingerprints(&ordered);
        for ((key, session), value) in keys.into_iter().zip(ordered).zip(values) {
            if !value.is_empty() {
                self.fingerprints.insert(key.clone(), value);
            }
            self.sessions.insert(key, session);
        }
    }

    pub fn fingerprint(&self, view: &MergedView) -> Option<String> {
        match view.pick {
            Pick::Remote => Some(view.content_hash.clone()),
            Pick::Local => self
                .fingerprints
                .get(&(view.harness, view.session_id.clone()))
                .cloned(),
            Pick::Ambiguous => None,
        }
    }

    pub async fn transcript(
        &mut self,
        r2: &R2,
        key: &Key,
        cache_dir: &Path,
        view: &MergedView,
    ) -> Result<Transcript<Common>> {
        let map_key = (view.harness, view.session_id.clone());
        if view.pick == Pick::Ambiguous {
            return Err(Error::Ambiguous {
                harness: view.harness.to_string(),
                session_id: view.session_id.clone(),
            });
        }
        if let Some(transcript) = self.loaded.get(&map_key) {
            return Ok(transcript.clone());
        }
        let transcript = match view.pick {
            Pick::Local => {
                let session = self.sessions.get(&map_key).ok_or_else(|| {
                    Error::msg(format!(
                        "local session {} {} disappeared",
                        view.harness, view.session_id
                    ))
                })?;
                session.read().map_err(|error| {
                    Error::msg(format!(
                        "reading {} {}: {error}",
                        view.harness, view.session_id
                    ))
                })?
            }
            Pick::Remote => {
                let object_key = view.object_key.as_deref().ok_or_else(|| {
                    Error::msg(format!(
                        "remote session {} {} has no object",
                        view.harness, view.session_id
                    ))
                })?;
                let plain =
                    load_plaintext(r2, key, cache_dir, object_key, &view.content_hash).await?;
                document_from_plaintext(&plain)?.into_transcript()?
            }
            Pick::Ambiguous => unreachable!("ambiguous sessions return before this match"),
        };
        self.loaded.insert(map_key, transcript.clone());
        Ok(transcript)
    }
}

pub async fn open_scope(
    r2: &R2,
    key: &Key,
    cwd: Option<&str>,
    from: Option<HarnessId>,
) -> Result<Scope> {
    let process = std::env::current_dir()?;
    let dir = scope_directory(cwd.map(Path::new), &process);
    if !dir.is_dir() {
        return Err(Error::msg(format!("{} is not a directory", dir.display())));
    }
    let repo_key =
        crate::repo_id::origin_of(&dir).map_err(|error| Error::msg(error.to_string()))?;
    let (catalog, _) = crate::remote::load_catalog(r2, key).await?;
    let repo_for_thread = repo_key.clone();
    let catalog_for_thread = catalog.clone();
    tokio::task::spawn_blocking(move || prepare(repo_for_thread, from, catalog_for_thread))
        .await
        .map_err(|error| Error::msg(format!("scanning local sessions: {error}")))?
}

fn prepare(repo_key: String, from: Option<HarnessId>, catalog: Catalog) -> Result<Scope> {
    let remotes = remote_views(&catalog)?;
    let mut held = discover_held()?;
    held = dedupe_locals(held)?;
    let mut loaded = HashMap::new();
    for item in &mut held {
        let Some(local_repo) = item.view.repo_key.clone() else {
            continue;
        };
        let Some(remote) = remotes.iter().find(|remote| {
            remote.repo_key == local_repo
                && remote.harness == item.view.harness
                && remote.session_id == item.view.session_id
        }) else {
            continue;
        };
        if needs_local_body(&item.view.freshness, &remote.freshness) {
            fill(item, &mut loaded)?;
        }
    }
    let locals: Vec<LocalView> = held.iter().map(|item| item.view.clone()).collect();
    let merged = select(&repo_key, from, &locals, &remotes)?;
    let selected: HashSet<_> = merged
        .iter()
        .filter(|view| view.pick == Pick::Local)
        .map(|view| (view.harness, view.session_id.clone()))
        .collect();
    let mut sessions = HashMap::new();
    for item in held {
        let identity = (item.view.harness, item.view.session_id);
        if selected.contains(&identity) {
            sessions.insert(identity, item.session);
        }
    }
    Ok(Scope {
        repo_key,
        merged,
        sessions,
        loaded,
        fingerprints: HashMap::new(),
    })
}

struct Held {
    session: Session,
    view: LocalView,
}

fn discover_held() -> Result<Vec<Held>> {
    let mut repos = RepoCache::default();
    let mut held = Vec::new();
    for session in local::discover() {
        if matches!(session.harness, HarnessId::ClaudeChat | HarnessId::ChatGpt) {
            continue;
        }
        let repo = repos.resolve(session.meta.cwd.as_deref())?;
        let repo_key = match repo {
            SessionRepo::Key(key) => Some(key),
            SessionRepo::MissingCwd | SessionRepo::Unresolved(_) => None,
        };
        let view = LocalView {
            harness: session.harness,
            session_id: session.meta.id.clone(),
            repo_key,
            freshness: Freshness {
                updated_at: session.updated_at,
                last_message_at: None,
                message_count: 0,
                content_hash: String::new(),
            },
            started_at: session.meta.timestamp,
            title: session.meta.title.clone(),
            cwd: session.meta.cwd.clone(),
            git_branch: session.meta.git_branch.clone(),
            model: session.meta.model.clone(),
        };
        held.push(Held { session, view });
    }
    Ok(held)
}

fn dedupe_locals(held: Vec<Held>) -> Result<Vec<Held>> {
    let mut groups: Vec<Vec<Held>> = Vec::new();
    for item in held {
        if let Some(group) = groups.iter_mut().find(|group| {
            group[0].view.harness == item.view.harness
                && group[0].view.session_id == item.view.session_id
                && group[0].view.repo_key == item.view.repo_key
        }) {
            group.push(item);
        } else {
            groups.push(vec![item]);
        }
    }
    groups.into_iter().map(choose_local).collect()
}

fn choose_local(mut group: Vec<Held>) -> Result<Held> {
    if group.len() == 1 || group[0].view.repo_key.is_none() {
        return Ok(group
            .into_iter()
            .max_by_key(|item| item.view.freshness.updated_at)
            .expect("group is non-empty"));
    }
    let dated = group
        .iter()
        .all(|item| item.view.freshness.updated_at.is_some());
    let first = group[0].view.freshness.updated_at;
    if dated
        && group
            .iter()
            .any(|item| item.view.freshness.updated_at != first)
    {
        return Ok(group
            .into_iter()
            .max_by_key(|item| item.view.freshness.updated_at)
            .expect("group is non-empty"));
    }
    let mut loaded = HashMap::new();
    for item in &mut group {
        fill(item, &mut loaded)?;
    }
    let mut best = group.remove(0);
    for other in group {
        match crate::merge::prefer(&best.view.freshness, &other.view.freshness) {
            Ok(crate::merge::Side::Local) => {}
            Ok(crate::merge::Side::Remote) => best = other,
            Err(_) => {
                return Err(Error::Ambiguous {
                    harness: best.view.harness.to_string(),
                    session_id: best.view.session_id,
                })
            }
        }
    }
    Ok(best)
}

fn needs_local_body(local: &Freshness, remote: &Freshness) -> bool {
    !matches!(
        (local.updated_at, remote.updated_at),
        (Some(left), Some(right)) if left != right
    )
}

fn fill(
    item: &mut Held,
    loaded: &mut HashMap<(HarnessId, String), Transcript<Common>>,
) -> Result<()> {
    let repo_key = item
        .view
        .repo_key
        .clone()
        .ok_or_else(|| Error::msg("cannot hash a session with no repo key"))?;
    let transcript = item.session.read().map_err(|error| {
        Error::msg(format!(
            "reading {} {}: {error}",
            item.view.harness, item.view.session_id
        ))
    })?;
    let document = ArchiveDocument::new(item.view.harness, repo_key, transcript.clone());
    item.view.freshness.last_message_at = document.messages.last().map(|message| message.timestamp);
    item.view.freshness.message_count = document.messages.len() as u64;
    item.view.freshness.content_hash = document.content_hash()?;
    item.view.title = document.meta.title.clone();
    item.view.cwd = document.meta.cwd.clone();
    item.view.git_branch = document.meta.git_branch.clone();
    item.view.model = document.meta.model.clone();
    item.view.started_at = document.meta.timestamp;
    loaded.insert(
        (item.view.harness, item.view.session_id.clone()),
        transcript,
    );
    Ok(())
}

fn remote_views(catalog: &Catalog) -> Result<Vec<RemoteView>> {
    let mut views = Vec::new();
    for session in &catalog.sessions {
        if session.revisions.is_empty() {
            return Err(Error::msg(format!(
                "session {} {} has no revisions",
                session.harness, session.session_id
            )));
        }
        let ranked = session
            .revisions
            .iter()
            .map(|revision| Freshness {
                updated_at: revision.updated_at,
                last_message_at: revision.last_message_at,
                message_count: revision.message_count,
                content_hash: revision.content_hash.clone(),
            })
            .collect::<Vec<_>>();
        let (index, ambiguous) = match choose_current(&ranked)? {
            Current::Index(index) => (index, false),
            Current::Ambiguous { display } => (display, true),
        };
        let current = &session.revisions[index];
        views.push(RemoteView {
            harness: session.harness,
            session_id: session.session_id.clone(),
            repo_key: session.repo_key.clone(),
            freshness: ranked[index].clone(),
            object_key: current.object_key.clone(),
            started_at: current.started_at,
            title: current.title.clone(),
            cwd: current.cwd.clone(),
            git_branch: current.git_branch.clone(),
            model: current.model.clone(),
            ambiguous,
        });
    }
    Ok(views)
}

pub fn find_session<'a>(
    merged: &'a [MergedView],
    query: &str,
) -> Result<(&'a MergedView, Option<crate::fragment::SpanReq>)> {
    if let Some(found) = unique_match(merged, query)? {
        return Ok((found, None));
    }
    let (src, range) = crate::fragment::parse_ref(query);
    let found = unique_match(merged, src)?
        .ok_or_else(|| Error::msg(format!("no session matches `{src}`")))?;
    Ok((found, range))
}

fn unique_match<'a>(merged: &'a [MergedView], src: &str) -> Result<Option<&'a MergedView>> {
    let exact: Vec<_> = merged
        .iter()
        .filter(|session| session.session_id == src)
        .collect();
    if exact.len() > 1 {
        return Err(Error::msg(format!("session id `{src}` is ambiguous")));
    }
    if exact.len() == 1 {
        return Ok(Some(exact[0]));
    }
    let prefix: Vec<_> = merged
        .iter()
        .filter(|session| session.session_id.starts_with(src) && !src.is_empty())
        .collect();
    if prefix.len() > 1 {
        return Err(Error::msg(format!(
            "session id prefix `{src}` is ambiguous"
        )));
    }
    if prefix.len() == 1 {
        return Ok(Some(prefix[0]));
    }
    let titles: Vec<_> = merged
        .iter()
        .filter(|session| session.title.as_deref() == Some(src))
        .collect();
    if titles.len() > 1 {
        return Err(Error::msg(format!("title `{src}` is ambiguous")));
    }
    if titles.len() == 1 {
        return Ok(Some(titles[0]));
    }
    Ok(None)
}
