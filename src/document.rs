use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use txcript::common::{Message, Meta};
use txcript::{Common, HarnessId, Transcript};

use crate::catalog::{object_key, Revision};
use crate::crypto::Key;
use crate::error::Result;
use crate::merge::{Freshness, Info};

/// Serialized body format version — independent of the catalog's schema:
/// the two persisted documents evolve separately.
pub const SCHEMA: u32 = 1;

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
        Ok(crate::fsutil::sha256_hex(&self.canonical_bytes()?))
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
            freshness: Freshness::of_body(&self.messages, updated_at, hash.to_string()),
            info: Info::of(&self.meta),
            size,
        })
    }
}

pub fn encrypt_document(key: &Key, object: &str, document: &ArchiveDocument) -> Result<Vec<u8>> {
    crate::crypto::encrypt(key, object, &document.canonical_bytes()?)
}
