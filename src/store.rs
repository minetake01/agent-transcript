use aws_sdk_s3::config::{
    Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::config::Config;
use crate::error::{Error, Result};

pub struct Fetched {
    pub body: Vec<u8>,
    pub etag: Option<String>,
}

pub enum Precondition {
    None,
    IfMatch(String),
    IfNoneMatchStar,
}

pub struct R2 {
    client: Client,
    bucket: String,
}

impl R2 {
    pub fn new(config: &Config) -> Self {
        let credentials = Credentials::new(
            &config.access_key_id,
            &config.secret_access_key,
            None,
            None,
            "agent-transcript",
        );
        let sdk = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("auto"))
            .endpoint_url(config.endpoint())
            .credentials_provider(credentials)
            .force_path_style(true)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build();
        Self {
            client: Client::from_conf(sdk),
            bucket: config.bucket.clone(),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Fetched>> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => {
                let etag = output.e_tag().map(str::to_string);
                let body = output
                    .body
                    .collect()
                    .await
                    .map_err(|error| Error::msg(format!("reading `{key}`: {error}")))?
                    .into_bytes()
                    .to_vec();
                Ok(Some(Fetched { body, etag }))
            }
            Err(error) if is_missing(&error) => Ok(None),
            Err(error) => Err(Error::msg(format!("getting `{key}`: {error}"))),
        }
    }

    pub async fn put(&self, key: &str, body: Vec<u8>, precondition: Precondition) -> Result<()> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body));
        request = match precondition {
            Precondition::None => request,
            Precondition::IfMatch(etag) => request.if_match(etag),
            Precondition::IfNoneMatchStar => request.if_none_match("*"),
        };
        match request.send().await {
            Ok(_) => Ok(()),
            Err(error) if is_precondition(&error) => Err(Error::Precondition),
            Err(error) => Err(Error::msg(format!("putting `{key}`: {error}"))),
        }
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = token.as_deref() {
                request = request.continuation_token(token);
            }
            let page = request
                .send()
                .await
                .map_err(|error| Error::msg(format!("listing `{prefix}`: {error}")))?;
            for object in page.contents() {
                if let Some(key) = object.key() {
                    keys.push(key.to_string());
                }
            }
            if page.is_truncated() == Some(true) {
                token = page.next_continuation_token().map(str::to_string);
                if token.is_none() {
                    return Err(Error::msg(format!(
                        "listing `{prefix}` was truncated without a continuation token"
                    )));
                }
            } else {
                break;
            }
        }
        Ok(keys)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| Error::msg(format!("deleting `{key}`: {error}")))?;
        Ok(())
    }
}

fn is_missing(error: &SdkError<aws_sdk_s3::operation::get_object::GetObjectError>) -> bool {
    match error {
        SdkError::ServiceError(service) => service.err().is_no_such_key(),
        _ => false,
    }
}

fn is_precondition(error: &SdkError<impl std::fmt::Debug>) -> bool {
    error
        .raw_response()
        .is_some_and(|raw| raw.status().as_u16() == 412)
}
