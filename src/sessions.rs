use std::path::Path;

use txcript::{Common, HarnessId, Transcript};

use crate::catalog::Catalog;
use crate::crypto::Key;
use crate::error::{Error, Result};
use crate::local_state::{LocalStore, SourceRecord};
use crate::merge::{
    choose_current, prefer, select, Current, LocalView, MergedView, Pick, RemoteView,
};
use crate::remote::{document_from_plaintext, load_plaintext};
use crate::sources::{self, LoadFailure};
use crate::store::R2;

/// Local session views grouped and deduplicated from the persistent records.
///
/// The records already carry freshness (timestamps, message count, content
/// hash), so ties are ranked by the same `prefer` the remote merge uses —
/// no body is read here. A group whose top-ranked copies disagree on the
/// body is flagged `ambiguous` on the view rather than failing the whole
/// listing.
pub fn local_views(store: &LocalStore) -> Vec<LocalView> {
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
            sources::load(view.harness, &source).map_err(|failure| load_error(view, failure))
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

/// Stat fingerprint used as the search-index key for a merged document.
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
        freshness: record.freshness.clone(),
        info: record.info.clone(),
        ambiguous: false,
    }
}

/// Pick the freshest copy of a session; when top-ranked copies disagree on
/// the body the view is flagged `ambiguous` — the same degraded state a
/// remote-side tie produces, not a listing error.
fn choose_local(mut group: Vec<LocalView>) -> LocalView {
    let mut best = group.remove(0);
    for other in group {
        match prefer(&best.freshness, &other.freshness) {
            Pick::Local => {}
            Pick::Remote => best = other,
            Pick::Ambiguous => best.ambiguous = true,
        }
    }
    best
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
            .map(|revision| revision.freshness.clone())
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
            freshness: current.freshness.clone(),
            object_key: current.object_key.clone(),
            info: current.info.clone(),
            ambiguous,
        });
    }
    Ok(views)
}

/// Find the session a query selects. Returns the view, the query component
/// that matched (without any `#range`), and the parsed range request.
pub fn find_session<'a>(
    merged: &'a [MergedView],
    query: &'a str,
) -> Result<(&'a MergedView, &'a str, Option<crate::fragment::SpanReq>)> {
    if let Some(found) = unique_match(merged, query)? {
        return Ok((found, query, None));
    }
    let (src, range) = crate::fragment::parse_ref(query);
    let found = unique_match(merged, src)?
        .ok_or_else(|| Error::msg(format!("no session matches `{src}`")))?;
    Ok((found, src, range))
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
        .filter(|session| session.info.title.as_deref() == Some(src))
        .collect();
    if titles.len() > 1 {
        return Err(Error::msg(format!("title `{src}` is ambiguous")));
    }
    if titles.len() == 1 {
        return Ok(Some(titles[0]));
    }
    Ok(None)
}
