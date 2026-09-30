use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use txcript::HarnessId;

use crate::error::{Error, Result};
use crate::merge::{choose_current, prefer, Current, Freshness, Info, Pick};

pub const CATALOG_KEY: &str = "v1/catalog";
/// Object-namespace prefix under which content-addressed bodies live.
pub const OBJECTS_PREFIX: &str = "v1/objects/";
pub const SCHEMA: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub schema: u32,
    pub sessions: Vec<SessionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub repo_key: String,
    pub harness: HarnessId,
    pub session_id: String,
    pub revisions: Vec<Revision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    pub object_key: String,
    #[serde(flatten)]
    pub freshness: Freshness,
    #[serde(flatten)]
    pub info: Info,
    /// Compressed ciphertext bytes stored under `object_key`. Zero for
    /// revisions written before the field existed.
    #[serde(default)]
    pub size: u64,
}

impl Catalog {
    pub fn empty() -> Self {
        Self {
            schema: SCHEMA,
            sessions: Vec::new(),
        }
    }
}

pub fn merge_catalogs(base: &Catalog, incoming: &Catalog) -> Result<Catalog> {
    if base.schema != SCHEMA {
        return Err(Error::Schema {
            schema: base.schema,
        });
    }
    if incoming.schema != SCHEMA {
        return Err(Error::Schema {
            schema: incoming.schema,
        });
    }
    let mut sessions = base.sessions.clone();
    for incoming_session in &incoming.sessions {
        if let Some(existing) = sessions.iter_mut().find(|session| {
            session.harness == incoming_session.harness
                && session.session_id == incoming_session.session_id
        }) {
            if existing.repo_key != incoming_session.repo_key {
                return Err(Error::msg(format!(
                    "session {} {} belongs to both `{}` and `{}`",
                    incoming_session.harness,
                    incoming_session.session_id,
                    existing.repo_key,
                    incoming_session.repo_key
                )));
            }
            for revision in &incoming_session.revisions {
                if !existing.revisions.iter().any(|current| {
                    current.freshness.content_hash == revision.freshness.content_hash
                }) {
                    existing.revisions.push(revision.clone());
                }
            }
        } else {
            sessions.push(incoming_session.clone());
        }
    }
    sessions.sort_by(|left, right| {
        left.harness
            .as_str()
            .cmp(right.harness.as_str())
            .then(left.session_id.cmp(&right.session_id))
    });
    for session in &mut sessions {
        session.revisions.sort_by(|left, right| {
            left.freshness
                .content_hash
                .cmp(&right.freshness.content_hash)
        });
        if session.revisions.is_empty() {
            return Err(Error::msg(format!(
                "session {} {} has no revisions",
                session.harness, session.session_id
            )));
        }
    }
    Ok(Catalog {
        schema: SCHEMA,
        sessions,
    })
}

pub fn object_key(content_hash: &str) -> Result<String> {
    if content_hash.len() < 3 || !content_hash.chars().all(|char| char.is_ascii_hexdigit()) {
        return Err(Error::msg(format!(
            "content hash `{content_hash}` is not hex"
        )));
    }
    Ok(format!(
        "{OBJECTS_PREFIX}sha256/{}/{}",
        &content_hash[..2],
        &content_hash[2..]
    ))
}

/// Drop revisions strictly superseded by the current one.
///
/// The winning revision and every revision tied with it at the top rank stay
/// — the ties are what remote ambiguity detection compares — while anything
/// clearly older is removed so `gc` can reclaim its object. Readers only ever
/// serve the current revision, so nothing user-visible is lost. Revisions
/// sharing a content hash collapse to one entry (same object anyway).
pub fn prune_to_current(catalog: &mut Catalog) {
    for session in &mut catalog.sessions {
        if session.revisions.len() <= 1 {
            continue;
        }
        let ranked = session
            .revisions
            .iter()
            .map(|revision| revision.freshness.clone())
            .collect::<Vec<_>>();
        let best = match choose_current(&ranked) {
            Ok(Current::Index(index)) | Ok(Current::Ambiguous { display: index }) => index,
            Err(_) => continue,
        };
        let mut by_hash: HashMap<&str, usize> = HashMap::new();
        for (index, revision) in session.revisions.iter().enumerate() {
            if matches!(prefer(&ranked[index], &ranked[best]), Pick::Remote) {
                continue;
            }
            match by_hash.get(revision.freshness.content_hash.as_str()) {
                None => {
                    by_hash.insert(revision.freshness.content_hash.as_str(), index);
                }
                // Prefer the winner's own metadata among same-body duplicates.
                Some(_) if index == best => {
                    by_hash.insert(revision.freshness.content_hash.as_str(), index);
                }
                Some(_) => {}
            }
        }
        let mut keep: Vec<usize> = by_hash.values().copied().collect();
        keep.sort_unstable();
        session.revisions = keep
            .iter()
            .map(|&index| session.revisions[index].clone())
            .collect();
    }
}

/// Total ciphertext bytes referenced by the catalog. Revisions written before
/// `size` existed count as zero, so this is a lower bound.
pub fn referenced_bytes(catalog: &Catalog) -> u64 {
    catalog
        .sessions
        .iter()
        .flat_map(|session| session.revisions.iter())
        .map(|revision| revision.size)
        .sum()
}

pub fn validate(catalog: &Catalog) -> Result<()> {
    if catalog.schema != SCHEMA {
        return Err(Error::Schema {
            schema: catalog.schema,
        });
    }
    for session in &catalog.sessions {
        if session.revisions.is_empty() {
            return Err(Error::msg(format!(
                "session {} {} has no revisions",
                session.harness, session.session_id
            )));
        }
        for revision in &session.revisions {
            let expected = object_key(&revision.freshness.content_hash)?;
            if revision.object_key != expected {
                return Err(Error::msg(format!(
                    "revision {} is stored at `{}` instead of `{expected}`",
                    revision.freshness.content_hash, revision.object_key
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn revision(hash: &str, messages: u64, updated: Option<i64>) -> Revision {
        Revision {
            object_key: object_key(hash).unwrap(),
            freshness: Freshness {
                content_hash: hash.to_string(),
                updated_at: updated.map(at),
                last_message_at: None,
                message_count: messages,
            },
            info: Info {
                title: None,
                started_at: at(1),
                cwd: None,
                git_branch: None,
                model: None,
            },
            size: 0,
        }
    }

    fn session(id: &str, revisions: Vec<Revision>) -> SessionRecord {
        SessionRecord {
            repo_key: "https://github.com/Org/Repo".into(),
            harness: HarnessId::Codex,
            session_id: id.into(),
            revisions,
        }
    }

    #[test]
    fn conflict_keeps_both_revisions() {
        let base = Catalog {
            schema: SCHEMA,
            sessions: vec![session("s1", vec![revision("aa11", 10, Some(1))])],
        };
        let incoming = Catalog {
            schema: SCHEMA,
            sessions: vec![session("s1", vec![revision("bb22", 3, Some(9))])],
        };
        let merged = merge_catalogs(&base, &incoming).unwrap();
        let record = &merged.sessions[0];
        assert_eq!(record.revisions.len(), 2);
        let freshness = record
            .revisions
            .iter()
            .map(|revision| revision.freshness.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            crate::merge::choose_current(&freshness).unwrap(),
            crate::merge::Current::Index(1)
        );
        assert_eq!(record.revisions[1].freshness.content_hash, "bb22");
    }

    #[test]
    fn merge_unions_disjoint_sessions() {
        let base = Catalog {
            schema: SCHEMA,
            sessions: vec![session("s1", vec![revision("aa11", 1, Some(1))])],
        };
        let mut other = session("s2", vec![revision("bb22", 1, Some(1))]);
        other.harness = HarnessId::Pi;
        let incoming = Catalog {
            schema: SCHEMA,
            sessions: vec![other],
        };
        let merged = merge_catalogs(&base, &incoming).unwrap();
        assert_eq!(merged.sessions.len(), 2);
    }

    #[test]
    fn schema_mismatch_fails() {
        let mut catalog = Catalog::empty();
        catalog.schema = 2;
        let error = merge_catalogs(&catalog, &Catalog::empty()).unwrap_err();
        assert!(matches!(error, Error::Schema { schema: 2 }));
    }

    #[test]
    fn prune_drops_superseded_but_keeps_ties() {
        let mut catalog = Catalog {
            schema: SCHEMA,
            sessions: vec![session(
                "s1",
                vec![
                    revision("aa11", 10, Some(1)),
                    revision("bb22", 3, Some(9)),
                    revision("cc33", 3, Some(9)),
                    revision("bb22", 3, Some(9)),
                ],
            )],
        };
        prune_to_current(&mut catalog);
        let hashes: Vec<&str> = catalog.sessions[0]
            .revisions
            .iter()
            .map(|r| r.freshness.content_hash.as_str())
            .collect();
        // aa11 is strictly older; bb22 and cc33 tie at the newest rank
        // (same time and count, different bodies) and the duplicate bb22
        // entry collapses.
        assert_eq!(hashes, ["bb22", "cc33"]);
    }

    #[test]
    fn prune_keeps_single_current_revision() {
        let mut catalog = Catalog {
            schema: SCHEMA,
            sessions: vec![session(
                "s1",
                vec![revision("aa11", 10, Some(1)), revision("bb22", 3, Some(9))],
            )],
        };
        prune_to_current(&mut catalog);
        assert_eq!(catalog.sessions[0].revisions.len(), 1);
        assert_eq!(
            catalog.sessions[0].revisions[0].freshness.content_hash,
            "bb22"
        );
    }
}
