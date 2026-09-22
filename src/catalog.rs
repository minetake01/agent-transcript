use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use txcript::HarnessId;

use crate::error::{Error, Result};

pub const CATALOG_KEY: &str = "v1/catalog";
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
    pub content_hash: String,
    pub object_key: String,
    pub title: Option<String>,
    pub started_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub model: Option<String>,
    pub message_count: u64,
}

impl Catalog {
    pub fn empty() -> Self {
        Self {
            schema: SCHEMA,
            sessions: Vec::new(),
        }
    }

    pub fn session(&self, harness: HarnessId, session_id: &str) -> Option<&SessionRecord> {
        self.sessions
            .iter()
            .find(|session| session.harness == harness && session.session_id == session_id)
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
                if !existing
                    .revisions
                    .iter()
                    .any(|current| current.content_hash == revision.content_hash)
                {
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
        session
            .revisions
            .sort_by(|left, right| left.content_hash.cmp(&right.content_hash));
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
        "v1/objects/sha256/{}/{}",
        &content_hash[..2],
        &content_hash[2..]
    ))
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
            let expected = object_key(&revision.content_hash)?;
            if revision.object_key != expected {
                return Err(Error::msg(format!(
                    "revision {} is stored at `{}` instead of `{expected}`",
                    revision.content_hash, revision.object_key
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn revision(hash: &str, messages: u64, updated: Option<i64>) -> Revision {
        Revision {
            content_hash: hash.to_string(),
            object_key: object_key(hash).unwrap(),
            title: None,
            started_at: at(1),
            updated_at: updated.map(at),
            last_message_at: None,
            cwd: None,
            git_branch: None,
            model: None,
            message_count: messages,
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
            .map(|revision| crate::merge::Freshness {
                updated_at: revision.updated_at,
                last_message_at: revision.last_message_at,
                message_count: revision.message_count,
                content_hash: revision.content_hash.clone(),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            crate::merge::choose_current(&freshness).unwrap(),
            crate::merge::Current::Index(1)
        );
        assert_eq!(record.revisions[1].content_hash, "bb22");
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
}
