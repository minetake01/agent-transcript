use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::RngCore;

use crate::error::{Error, Result};

pub const KEY_LEN: usize = 32;
const MAGIC: &[u8] = b"ATX1";
const NONCE_LEN: usize = 24;

pub type Key = [u8; KEY_LEN];

pub fn generate_key() -> Key {
    let mut key = [0u8; KEY_LEN];
    rand::rngs::OsRng.fill_bytes(&mut key);
    key
}

pub fn encrypt(key: &Key, object_key: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).map_err(|_| Error::msg("invalid encryption key"))?;
    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: object_key.as_bytes(),
            },
        )
        .map_err(|_| Error::Crypto {
            key: object_key.to_string(),
        })?;
    let mut out = Vec::with_capacity(MAGIC.len() + NONCE_LEN + ciphertext.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

pub fn decrypt(key: &Key, object_key: &str, blob: &[u8]) -> Result<Vec<u8>> {
    if blob.len() < MAGIC.len() + NONCE_LEN + 16 || &blob[..MAGIC.len()] != MAGIC {
        return Err(Error::Format {
            key: object_key.to_string(),
        });
    }
    let nonce = &blob[MAGIC.len()..MAGIC.len() + NONCE_LEN];
    let ciphertext = &blob[MAGIC.len() + NONCE_LEN..];
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).map_err(|_| Error::msg("invalid encryption key"))?;
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: object_key.as_bytes(),
            },
        )
        .map_err(|_| Error::Crypto {
            key: object_key.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Key {
        let mut key = [0u8; KEY_LEN];
        key[0] = 7;
        key[31] = 9;
        key
    }

    #[test]
    fn round_trip_restores_plaintext() {
        let blob = encrypt(&key(), "v1/catalog", b"session-body").unwrap();
        let plain = decrypt(&key(), "v1/catalog", &blob).unwrap();
        assert_eq!(plain, b"session-body");
    }

    #[test]
    fn tampered_ciphertext_fails_authentication() {
        let mut blob = encrypt(&key(), "v1/objects/sha256/aa/rest", b"session-body").unwrap();
        let last = blob.len() - 1;
        blob[last] ^= 0x01;
        let error = decrypt(&key(), "v1/objects/sha256/aa/rest", &blob).unwrap_err();
        assert!(matches!(error, Error::Crypto { .. }));
    }

    #[test]
    fn associated_data_mismatch_fails_authentication() {
        let blob = encrypt(&key(), "v1/catalog", b"session-body").unwrap();
        let error = decrypt(&key(), "v1/other", &blob).unwrap_err();
        assert!(matches!(error, Error::Crypto { .. }));
    }

    #[test]
    fn unrecognized_header_is_a_format_error() {
        let mut blob = encrypt(&key(), "v1/catalog", b"session-body").unwrap();
        blob[0] = b'X';
        let error = decrypt(&key(), "v1/catalog", &blob).unwrap_err();
        assert!(matches!(error, Error::Format { .. }));
    }
}
