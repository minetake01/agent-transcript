use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use txcript::common::{Message, Meta};
use txcript::{Common, HarnessId, Transcript};

use crate::catalog::{object_key, Revision, SCHEMA};
use crate::crypto::Key;
use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArchiveDocument {
    pub schema: u32,
    pub harness: HarnessId,
    pub repo_key: String,
    pub meta: Meta,
    pub messages: Vec<Message>,
}

impl ArchiveDocument {
    pub fn new(harness: HarnessId, repo_key: String, transcript: Transcript<Common>) -> Self {
        Self {
            schema: SCHEMA,
            harness,
            repo_key,
            meta: transcript.meta,
            messages: transcript.body,
        }
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn content_hash(&self) -> Result<String> {
        let digest = Sha256::digest(self.canonical_bytes()?);
        Ok(hex::encode(digest))
    }

    pub fn into_transcript(self) -> Result<Transcript<Common>> {
        if self.schema != SCHEMA {
            return Err(crate::error::Error::Schema {
                schema: self.schema,
            });
        }
        Ok(Transcript::new(self.meta, self.messages))
    }

    pub fn revision(
        &self,
        hash: &str,
        updated_at: Option<DateTime<Utc>>,
        size: u64,
    ) -> Result<Revision> {
        Ok(Revision {
            object_key: object_key(hash)?,
            freshness: crate::merge::Freshness {
                updated_at,
                last_message_at: self.messages.last().map(|message| message.timestamp),
                message_count: self.messages.len() as u64,
                content_hash: hash.to_string(),
            },
            info: crate::merge::Info {
                started_at: self.meta.timestamp,
                title: self.meta.title.clone(),
                cwd: self.meta.cwd.clone(),
                git_branch: self.meta.git_branch.clone(),
                model: self.meta.model.clone(),
            },
            size,
        })
    }
}

pub fn encrypt_document(key: &Key, object: &str, document: &ArchiveDocument) -> Result<Vec<u8>> {
    crate::crypto::encrypt(key, object, &document.canonical_bytes()?)
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
