use std::time::Duration;

use chrono::{DateTime, Utc};
use ureq::http::{Method, Response};
use ureq::{Agent, Body};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::sigv4;

/// One object from a bucket listing.
pub struct Listed {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<DateTime<Utc>>,
}

pub struct Fetched {
    pub body: Vec<u8>,
    pub etag: Option<String>,
}

pub enum Precondition {
    None,
    IfMatch(String),
    IfNoneMatchStar,
}

#[derive(Clone)]
pub struct R2 {
    agent: Agent,
    endpoint: String,
    host: String,
    bucket: String,
    access_key_id: String,
    secret_access_key: String,
}

impl R2 {
    pub fn new(config: &Config) -> Self {
        let endpoint = config.endpoint();
        let host = endpoint
            .trim_start_matches("https://")
            .trim_end_matches('/')
            .to_string();
        let agent = Agent::config_builder()
            .user_agent(concat!("agent-transcript/", env!("CARGO_PKG_VERSION")))
            .timeout_global(Some(Duration::from_secs(300)))
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            agent,
            endpoint,
            host,
            bucket: config.bucket.clone(),
            access_key_id: config.access_key_id.clone(),
            secret_access_key: config.secret_access_key.clone(),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Fetched>> {
        let key = key.to_string();
        self.blocking(move |this| this.get_sync(&key)).await
    }

    pub async fn put(&self, key: &str, body: Vec<u8>, precondition: Precondition) -> Result<()> {
        let key = key.to_string();
        self.blocking(move |this| this.put_sync(&key, &body, &precondition))
            .await
    }

    /// The object's current ETag without downloading it. `None` when the key
    /// does not exist.
    pub async fn head_etag(&self, key: &str) -> Result<Option<String>> {
        let key = key.to_string();
        self.blocking(move |this| this.head_etag_sync(&key)).await
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_detailed(prefix)
            .await?
            .into_iter()
            .map(|object| object.key)
            .collect())
    }

    pub async fn list_detailed(&self, prefix: &str) -> Result<Vec<Listed>> {
        let prefix = prefix.to_string();
        self.blocking(move |this| this.list_detailed_sync(&prefix))
            .await
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let key = key.to_string();
        self.blocking(move |this| this.delete_sync(&key)).await
    }

    /// Run one blocking request on the blocking thread pool. The public API
    /// stays async because callers time out and race these operations.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&R2) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || f(&this))
            .await
            .map_err(|error| Error::msg(format!("r2 request task failed: {error}")))?
    }

    /// Send a signed request. `uri` is the canonical (already-encoded) object
    /// or bucket path, `query` the canonical query string, and `extra`
    /// additional lowercase-named headers to sign and send.
    fn request(
        &self,
        method: Method,
        uri: &str,
        query: &str,
        extra: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response<Body>> {
        let signed = sigv4::sign(
            &self.access_key_id,
            &self.secret_access_key,
            method.as_str(),
            &self.host,
            uri,
            query,
            extra,
            body.unwrap_or(&[]),
        );
        let mut url = format!("{}{}", self.endpoint, uri);
        if !query.is_empty() {
            url.push('?');
            url.push_str(query);
        }
        let mut builder = ureq::http::Request::builder()
            .method(method.clone())
            .uri(url)
            .header("x-amz-date", &signed.date)
            .header("x-amz-content-sha256", &signed.payload_hash);
        for (name, value) in extra {
            builder = builder.header(*name, *value);
        }
        builder = builder.header("authorization", &signed.authorization);
        let response = match body {
            None => self.agent.run(
                builder
                    .body(())
                    .map_err(|error| Error::msg(error.to_string()))?,
            ),
            Some(body) => self.agent.run(
                builder
                    .body(body)
                    .map_err(|error| Error::msg(error.to_string()))?,
            ),
        };
        response.map_err(|error| Error::msg(format!("{method} {uri}: {error}")))
    }

    /// The canonical object path `/{bucket}/{key}` with each segment encoded.
    fn object_uri(&self, key: &str) -> String {
        format!(
            "/{}/{}",
            sigv4::encode(&self.bucket, false),
            sigv4::encode(key, true)
        )
    }

    fn get_sync(&self, key: &str) -> Result<Option<Fetched>> {
        let uri = self.object_uri(key);
        let mut response = self.request(Method::GET, &uri, "", &[], None)?;
        match response.status().as_u16() {
            200..=299 => {
                let etag = etag(&response);
                let body = response
                    .body_mut()
                    .read_to_vec()
                    .map_err(|error| Error::msg(format!("reading `{key}`: {error}")))?;
                Ok(Some(Fetched { body, etag }))
            }
            404 => Ok(None),
            _ => Err(response_error("getting", key, &mut response)),
        }
    }

    fn put_sync(&self, key: &str, body: &[u8], precondition: &Precondition) -> Result<()> {
        let extra: Vec<(&str, &str)> = match precondition {
            Precondition::None => vec![],
            Precondition::IfMatch(etag) => vec![("if-match", etag.as_str())],
            Precondition::IfNoneMatchStar => vec![("if-none-match", "*")],
        };
        let uri = self.object_uri(key);
        let mut response = self.request(Method::PUT, &uri, "", &extra, Some(body))?;
        match response.status().as_u16() {
            200..=299 => Ok(()),
            412 => Err(Error::Precondition),
            _ => Err(response_error("putting", key, &mut response)),
        }
    }

    fn head_etag_sync(&self, key: &str) -> Result<Option<String>> {
        let uri = self.object_uri(key);
        let mut response = self.request(Method::HEAD, &uri, "", &[], None)?;
        match response.status().as_u16() {
            200..=299 => Ok(etag(&response)),
            404 => Ok(None),
            _ => Err(response_error("heading", key, &mut response)),
        }
    }

    fn list_detailed_sync(&self, prefix: &str) -> Result<Vec<Listed>> {
        let bucket_uri = format!("/{}", sigv4::encode(&self.bucket, false));
        let mut objects = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut pairs = vec![("list-type", "2"), ("prefix", prefix)];
            if let Some(token) = token.as_deref() {
                pairs.push(("continuation-token", token));
            }
            let query = sigv4::canonical_query(&pairs);
            let mut response = self.request(Method::GET, &bucket_uri, &query, &[], None)?;
            if !response.status().is_success() {
                return Err(response_error("listing", prefix, &mut response));
            }
            let xml = response
                .body_mut()
                .read_to_string()
                .map_err(|error| Error::msg(format!("listing `{prefix}`: {error}")))?;
            let (mut page, truncated, next) = parse_listing(&xml, prefix)?;
            objects.append(&mut page);
            if !truncated {
                break;
            }
            token = Some(next.ok_or_else(|| {
                Error::msg(format!(
                    "listing `{prefix}` was truncated without a continuation token"
                ))
            })?);
        }
        Ok(objects)
    }

    fn delete_sync(&self, key: &str) -> Result<()> {
        let uri = self.object_uri(key);
        let mut response = self.request(Method::DELETE, &uri, "", &[], None)?;
        match response.status().as_u16() {
            200..=299 => Ok(()),
            _ => Err(response_error("deleting", key, &mut response)),
        }
    }
}

fn etag(response: &Response<Body>) -> Option<String> {
    response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// Format a non-success response into an error that carries the S3 error body
/// (draining it keeps the connection reusable).
fn response_error(op: &str, target: &str, response: &mut Response<Body>) -> Error {
    let status = response.status().as_u16();
    let body = response
        .body_mut()
        .read_to_string()
        .unwrap_or_else(|_| String::new());
    let detail: String = body.trim().chars().take(300).collect();
    Error::msg(format!("{op} `{target}`: HTTP {status} {detail}"))
}

/// Parse a `ListObjectsV2` response body into the page's objects, the
/// truncation flag, and the continuation token.
fn parse_listing(
    xml: &str,
    prefix: &str,
) -> Result<(Vec<Listed>, bool, Option<String>)> {
    use xmlparser::{ElementEnd, Token, Tokenizer};

    let invalid = |error: xmlparser::Error| {
        Error::msg(format!("listing `{prefix}` returned invalid XML: {error}"))
    };
    let mut objects = Vec::new();
    let mut truncated = false;
    let mut next_token = None;
    let mut in_contents = false;
    let mut current = Listed {
        key: String::new(),
        size: 0,
        last_modified: None,
    };
    let mut text = String::new();
    for token in Tokenizer::from(xml) {
        match token.map_err(invalid)? {
            Token::ElementStart { local, .. } => {
                text.clear();
                if local.as_str() == "Contents" {
                    in_contents = true;
                    current = Listed {
                        key: String::new(),
                        size: 0,
                        last_modified: None,
                    };
                }
            }
            Token::Text { text: part } => text.push_str(part.as_str()),
            Token::Cdata { text: part, .. } => text.push_str(part.as_str()),
            Token::ElementEnd {
                end: ElementEnd::Close(_, local),
                ..
            } => {
                let name = local.as_str();
                if in_contents {
                    match name {
                        "Key" => current.key = unescape(text.trim()),
                        "Size" => {
                            current.size = text.trim().parse().map_err(|_| {
                                Error::msg(format!(
                                    "listing `{prefix}` has a non-numeric object size"
                                ))
                            })?
                        }
                        "LastModified" => {
                            current.last_modified = DateTime::parse_from_rfc3339(text.trim())
                                .ok()
                                .map(|date| date.with_timezone(&Utc))
                        }
                        "Contents" => {
                            in_contents = false;
                            objects.push(Listed {
                                key: std::mem::take(&mut current.key),
                                size: current.size,
                                last_modified: current.last_modified,
                            });
                        }
                        _ => {}
                    }
                } else {
                    match name {
                        "IsTruncated" => truncated = text.trim() == "true",
                        "NextContinuationToken" => next_token = Some(unescape(text.trim())),
                        _ => {}
                    }
                }
                text.clear();
            }
            _ => {}
        }
    }
    Ok((objects, truncated, next_token))
}

/// Decode the XML predefined entities and numeric character references that
/// may appear in element text (object keys can contain `&`, `<`, ...).
fn unescape(text: &str) -> String {
    if !text.contains('&') {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        let Some(end) = tail.find(';') else {
            out.push_str(tail);
            return out;
        };
        let entity = &tail[1..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .and_then(|digits| u32::from_str_radix(digits, 16).ok())
                .or_else(|| {
                    entity
                        .strip_prefix('#')
                        .and_then(|digits| digits.parse::<u32>().ok())
                })
                .and_then(char::from_u32),
        };
        match decoded {
            Some(character) => out.push(character),
            None => out.push_str(&tail[..=end]),
        }
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_listing_reads_pages_and_continuation() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>v1/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <Contents>
    <Key>v1/catalog</Key>
    <LastModified>2026-09-30T12:34:56.000Z</LastModified>
    <ETag>"abc"</ETag>
    <Size>128</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>v1/objects/sha256/aa/a&amp;b</Key>
    <LastModified>2026-09-29T00:00:00.000Z</LastModified>
    <ETag>"def"</ETag>
    <Size>64</Size>
  </Contents>
  <NextContinuationToken>token+with/slashes</NextContinuationToken>
</ListBucketResult>"#;
        let (objects, truncated, next) = parse_listing(xml, "v1/").unwrap();
        assert!(truncated);
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].key, "v1/catalog");
        assert_eq!(objects[0].size, 128);
        assert!(objects[0].last_modified.is_some());
        assert_eq!(objects[1].key, "v1/objects/sha256/aa/a&b");
        assert_eq!(next.as_deref(), Some("token+with/slashes"));
    }

    #[test]
    fn parse_listing_stops_when_not_truncated() {
        let xml = r#"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let (objects, truncated, next) = parse_listing(xml, "").unwrap();
        assert!(objects.is_empty());
        assert!(!truncated);
        assert!(next.is_none());
    }

    #[test]
    fn unescape_decodes_named_and_numeric_entities() {
        assert_eq!(
            unescape("a&amp;b&lt;c&gt;d&quot;e&apos;"),
            "a&b<c>d\"e'"
        );
        assert_eq!(unescape("x&#65;&#x42;"), "xAB");
        assert_eq!(unescape("plain"), "plain");
        assert_eq!(unescape("a&b"), "a&b");
    }
}
