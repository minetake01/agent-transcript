use std::path::Path;

use txcript::{Common, HarnessId, Transcript};

use crate::catalog::Catalog;
use crate::crypto::Key;
use crate::document::ArchiveDocument;
use crate::error::{Error, Result};
use crate::local_state::{LocalStore, SourceRecord};
use crate::merge::{
    choose_current, select, Current, Freshness, LocalView, MergedView, Pick, RemoteView,
};
use crate::remote::{document_from_plaintext, load_plaintext};
use crate::sources::{self, Candidate, LoadFailure};
use crate::store::R2;

/// Local session views grouped and deduplicated from the persistent records.
///
/// Bodies are only read here to break exact metadata ties between two
/// records of the same session (mirrors the previous `fill()` behavior);
/// everything else is answered from the stored record metadata.
pub fn local_views(store: &LocalStore) -> Result<Vec<LocalView>> {
    store
        .grouped()
        .into_values()
        .map(|group| choose_local(group.into_iter().map(view_of).collect()))
        .collect()
}

/// Merge local views with the catalog's remote revisions for one repository.
pub fn merged(
    repo_key: &str,
    from: Option<HarnessId>,
    locals: &[LocalView],
    catalog: &Catalog,
) -> Result<Vec<MergedView>> {
    let remotes = remote_views(catalog)?;
    select(repo_key, from, locals, &remotes)
}

/// Read the body a merged view points at.
///
/// `Local` reads the source on disk right now — local sessions are always
/// fresh. `Remote` reads the encrypted object through the plaintext cache.
pub async fn transcript(
    r2: &R2,
    key: &Key,
    cache_dir: &Path,
    view: &MergedView,
) -> Result<Transcript<Common>> {
    match view.pick {
        Pick::Ambiguous => Err(Error::Ambiguous {
            harness: view.harness.to_string(),
            session_id: view.session_id.clone(),
        }),
        Pick::Local => {
            let source = view.local_source.clone().ok_or_else(|| {
                Error::msg(format!(
                    "local session {} {} has no source",
                    view.harness, view.session_id
                ))
            })?;
            let candidate = Candidate {
                harness: view.harness,
                source,
                fingerprint: view.local_fingerprint.clone().unwrap_or_default(),
            };
            sources::load(&candidate)
                .map(|loaded| loaded.transcript)
                .map_err(|failure| load_error(view, failure))
        }
        Pick::Remote => {
            let object_key = view.object_key.as_deref().ok_or_else(|| {
                Error::msg(format!(
                    "remote session {} {} has no object",
                    view.harness, view.session_id
                ))
            })?;
            let plain = load_plaintext(r2, key, cache_dir, object_key, &view.content_hash).await?;
            document_from_plaintext(&plain)?.into_transcript()
        }
    }
}

/// Stat fingerprint used as the search-cache key for a merged document.
/// Remote documents key on their content hash; local documents on the
/// source fingerprint recorded by the local state store.
pub fn fingerprint_of(view: &MergedView) -> String {
    match view.pick {
        Pick::Remote => view.content_hash.clone(),
        Pick::Local => view.local_fingerprint.clone().unwrap_or_default(),
        Pick::Ambiguous => String::new(),
    }
}

fn view_of(record: &SourceRecord) -> LocalView {
    LocalView {
        harness: record.harness,
        session_id: record.session_id.clone(),
        repo_key: record.repo_key.clone(),
        source: record.source.clone(),
        fingerprint: record.fingerprint.clone(),
        freshness: Freshness {
            updated_at: record.updated_at,
            last_message_at: record.last_message_at,
            message_count: record.message_count,
            content_hash: record.content_hash.clone(),
        },
        started_at: record.started_at,
        title: record.title.clone(),
        cwd: record.cwd.clone(),
        git_branch: record.git_branch.clone(),
        model: record.model.clone(),
    }
}

fn choose_local(mut group: Vec<LocalView>) -> Result<LocalView> {
    if group.len() == 1 || group[0].repo_key.is_none() {
        return Ok(group
            .into_iter()
            .max_by_key(|view| view.freshness.updated_at)
            .expect("group is non-empty"));
    }
    let dated = group.iter().all(|view| view.freshness.updated_at.is_some());
    let first = group[0].freshness.updated_at;
    if dated && group.iter().any(|view| view.freshness.updated_at != first) {
        return Ok(group
            .into_iter()
            .max_by_key(|view| view.freshness.updated_at)
            .expect("group is non-empty"));
    }
    for view in &mut group {
        fill(view)?;
    }
    let mut best = group.remove(0);
    for other in group {
        match crate::merge::prefer(&best.freshness, &other.freshness) {
            Ok(crate::merge::Side::Local) => {}
            Ok(crate::merge::Side::Remote) => best = other,
            Err(_) => {
                return Err(Error::Ambiguous {
                    harness: best.harness.to_string(),
                    session_id: best.session_id,
                })
            }
        }
    }
    Ok(best)
}

/// Tie resolution: read the body once to rank two equally dated records of
/// the same session.
fn fill(view: &mut LocalView) -> Result<()> {
    let repo_key = view
        .repo_key
        .clone()
        .ok_or_else(|| Error::msg("cannot hash a session with no repo key"))?;
    let candidate = Candidate {
        harness: view.harness,
        source: view.source.clone(),
        fingerprint: view.fingerprint.clone(),
    };
    let transcript = sources::load(&candidate)
        .map(|loaded| loaded.transcript)
        .map_err(|failure| {
            Error::msg(format!(
                "reading {} {}: {}",
                view.harness,
                view.session_id,
                match failure {
                    LoadFailure::Transient(message) | LoadFailure::Broken(message) => message,
                }
            ))
        })?;
    let document = ArchiveDocument::new(view.harness, repo_key, transcript);
    view.freshness.last_message_at = document.messages.last().map(|message| message.timestamp);
    view.freshness.message_count = document.messages.len() as u64;
    view.freshness.content_hash = document.content_hash()?;
    view.title = document.meta.title.clone();
    view.cwd = document.meta.cwd.clone();
    view.git_branch = document.meta.git_branch.clone();
    view.model = document.meta.model.clone();
    view.started_at = document.meta.timestamp;
    Ok(())
}

fn load_error(view: &MergedView, failure: LoadFailure) -> Error {
    let message = match failure {
        LoadFailure::Transient(message) | LoadFailure::Broken(message) => message,
    };
    Error::msg(format!(
        "reading {} {}: {message}",
        view.harness, view.session_id
    ))
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
