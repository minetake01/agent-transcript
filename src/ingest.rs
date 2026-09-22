use crate::catalog::{Catalog, SessionRecord, SCHEMA};
use crate::config;
use crate::crypto::Key;
use crate::document::{encrypt_document, ArchiveDocument};
use crate::error::{Error, Result};
use crate::remote::{commit_catalog, load_catalog};
use crate::repo_id::{session_repo, SessionRepo};
use crate::store::{Precondition, R2};
use txcript::local::{self, Session};
use txcript::HarnessId;

struct Upload {
    object_key: String,
    blob: Vec<u8>,
}

struct Scan {
    uploads: Vec<Upload>,
    incoming: Catalog,
    unchanged: usize,
    missing_cwd: usize,
    unresolved: Vec<String>,
}

pub async fn ingest() -> Result<()> {
    let config = config::load_config()?;
    config.require_write()?;
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let scan = tokio::task::spawn_blocking(move || scan(catalog, key))
        .await
        .map_err(|error| Error::msg(format!("scanning local sessions: {error}")))??;
    for upload in &scan.uploads {
        r2.put(&upload.object_key, upload.blob.clone(), Precondition::None)
            .await?;
    }
    commit_catalog(&r2, &config::load_key()?, &scan.incoming).await?;
    println!(
        "uploaded {} session(s), unchanged {}, missing cwd {}",
        scan.uploads.len(),
        scan.unchanged,
        scan.missing_cwd
    );
    if !scan.unresolved.is_empty() {
        return Err(Error::msg(format!(
            "origin could not be resolved for {} session(s):\n{}",
            scan.unresolved.len(),
            scan.unresolved.join("\n")
        )));
    }
    Ok(())
}

pub async fn gc() -> Result<()> {
    let config = config::load_config()?;
    config.require_write()?;
    let key = config::load_key()?;
    let r2 = R2::new(&config);
    let (catalog, _) = load_catalog(&r2, &key).await?;
    let mut live = std::collections::HashSet::new();
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
    println!("deleted {deleted} unreferenced object(s)");
    Ok(())
}

fn scan(catalog: Catalog, key: Key) -> Result<Scan> {
    let mut scan = Scan {
        uploads: Vec::new(),
        incoming: Catalog::empty(),
        unchanged: 0,
        missing_cwd: 0,
        unresolved: Vec::new(),
    };
    for session in local::discover() {
        if matches!(session.harness, HarnessId::ClaudeChat | HarnessId::ChatGpt) {
            continue;
        }
        match session_repo(session.meta.cwd.as_deref())? {
            SessionRepo::MissingCwd => scan.missing_cwd += 1,
            SessionRepo::Unresolved(reason) => {
                scan.unresolved.push(format!(
                    "{} {} — {reason}",
                    session.harness, session.meta.id
                ));
            }
            SessionRepo::Key(repo_key) => {
                push_session(&mut scan, &catalog, &key, session, repo_key)?
            }
        }
    }
    Ok(scan)
}

fn push_session(
    scan: &mut Scan,
    catalog: &Catalog,
    key: &Key,
    session: Session,
    repo_key: String,
) -> Result<()> {
    let transcript = session.read().map_err(|error| {
        Error::msg(format!(
            "reading {} {}: {error}",
            session.harness, session.meta.id
        ))
    })?;
    let harness = session.harness;
    let session_id = session.meta.id.clone();
    let updated_at = session.updated_at;
    let document = ArchiveDocument::new(harness, repo_key.clone(), transcript);
    let hash = document.content_hash()?;
    if known(catalog, &scan.incoming, harness, &session_id, &hash) {
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
            session_id,
            revisions: vec![revision],
        }],
    };
    scan.incoming = crate::catalog::merge_catalogs(&scan.incoming, &piece)?;
    scan.uploads.push(Upload { object_key, blob });
    Ok(())
}

fn known(
    catalog: &Catalog,
    incoming: &Catalog,
    harness: HarnessId,
    session_id: &str,
    hash: &str,
) -> bool {
    let contains = |source: &Catalog| {
        source.session(harness, session_id).is_some_and(|session| {
            session
                .revisions
                .iter()
                .any(|revision| revision.content_hash == hash)
        })
    };
    contains(catalog) || contains(incoming)
}
