//! Minimal AWS Signature Version 4 signing for Cloudflare R2 (S3-compatible,
//! region `auto`, path-style URLs, static credentials).
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const REGION: &str = "auto";
const SERVICE: &str = "s3";

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// RFC 3986 percent-encoding for SigV4: unreserved characters pass through,
/// every other byte becomes `%XX`. `keep_slash` preserves `/` separators in
/// object paths; query names and values must not preserve it.
pub fn encode(input: &str, keep_slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// Sorted `name=value&...` canonical query built from unencoded pairs. The
/// returned string is used verbatim both in the canonical request and on the
/// wire, which keeps the signature and the request consistent by construction.
pub fn canonical_query(pairs: &[(&str, &str)]) -> String {
    let mut encoded: Vec<String> = pairs
        .iter()
        .map(|(name, value)| format!("{}={}", encode(name, false), encode(value, false)))
        .collect();
    encoded.sort();
    encoded.join("&")
}

pub struct Signed {
    /// Value for the `x-amz-date` header.
    pub date: String,
    /// Value for the `x-amz-content-sha256` header.
    pub payload_hash: String,
    /// Value for the `authorization` header.
    pub authorization: String,
}

/// Sign one request. `uri` and `query` are the already-encoded canonical path
/// and query; `extra_headers` are additional lowercase-named headers that are
/// both sent and signed (e.g. `if-match`). `host` is sent implicitly by the
/// HTTP stack, so it is always part of the signature.
pub fn sign(
    access_key_id: &str,
    secret_access_key: &str,
    method: &str,
    host: &str,
    uri: &str,
    query: &str,
    extra_headers: &[(&str, &str)],
    payload: &[u8],
) -> Signed {
    let now: DateTime<Utc> = Utc::now();
    let date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let scope_date = now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(payload));

    let mut headers: Vec<(&str, &str)> = vec![
        ("host", host),
        ("x-amz-content-sha256", payload_hash.as_str()),
        ("x-amz-date", date.as_str()),
    ];
    headers.extend_from_slice(extra_headers);
    headers.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{}:{}\n", name, value.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let scope = format!("{scope_date}/{REGION}/{SERVICE}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let k_date = hmac(format!("AWS4{secret_access_key}").as_bytes(), &scope_date);
    let k_region = hmac(&k_date, REGION);
    let k_service = hmac(&k_region, SERVICE);
    let k_signing = hmac(&k_service, "aws4_request");
    let signature = hex::encode(hmac(&k_signing, &string_to_sign));

    Signed {
        date,
        payload_hash,
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_leaves_unreserved_and_encodes_the_rest() {
        assert_eq!(encode("v1/objects/sha256/ab/c-d_e.f~g", true), "v1/objects/sha256/ab/c-d_e.f~g");
        assert_eq!(encode("a b+c&d", true), "a%20b%2Bc%26d");
        assert_eq!(encode("a/b", false), "a%2Fb");
    }

    #[test]
    fn canonical_query_sorts_and_encodes() {
        assert_eq!(
            canonical_query(&[("prefix", "v1/"), ("list-type", "2")]),
            "list-type=2&prefix=v1%2F"
        );
    }

    // SigV4 example from the AWS documentation (GET IAM Action=ListUsers).
    #[test]
    fn signature_matches_aws_example_key_derivation() {
        // The AWS doc example signs at 20150830T123600Z; our signer stamps the
        // current time, so verify the deterministic HMAC chain directly.
        let k_date = hmac(b"AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830");
        let k_region = hmac(&k_date, "us-east-1");
        let k_service = hmac(&k_region, "iam");
        let k_signing = hmac(&k_service, "aws4_request");
        assert_eq!(
            hex::encode(k_signing),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }
}
