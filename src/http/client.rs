// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::STORE;
use crate::client::get::{GetClient, IfRange, validate_identity_encoding};
use crate::client::header::HeaderConfig;
use crate::client::retry::{self, RetryConfig, RetryContext, RetryExt};
use crate::client::{GetOptionsExt, HttpClient, HttpError, HttpResponse};
use crate::path::{DELIMITER, Path};
use crate::util::deserialize_rfc1123;
use crate::{
    Attribute, Attributes, ClientOptions, GetOptions, GetRange, ObjectMeta, PutPayload, Result,
};
use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use chrono::{DateTime, Utc};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use http::header::ACCEPT_ENCODING;
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG,
};
use http::{Method, StatusCode};
use http_body_util::BodyExt;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use url::Url;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Request error: {}", source)]
    Request {
        source: retry::RetryError,
        path: String,
    },

    #[error("Request error: {}", source)]
    Reqwest { source: HttpError },

    #[error("Server did not honor the range request for {href}")]
    RangeNotSupported { href: String },

    #[error(
        "Server did not honor If-Range for {href}; expected validator {expected}, received {actual:?}"
    )]
    RangeValidatorChanged {
        href: String,
        expected: String,
        actual: Option<String>,
    },

    #[error(
        "Cannot buffer range fallback for {href}: {object_size}-byte object exceeds configured {max_size}-byte limit"
    )]
    RangeFallbackTooLarge {
        href: String,
        object_size: u64,
        max_size: u64,
    },

    #[error("Range fallback for {href} declared {declared} bytes but returned {actual} bytes")]
    RangeFallbackLength {
        href: String,
        declared: u64,
        actual: u64,
    },

    #[error(
        "Range fallback for {href} omitted Content-Length; for browser requests verify CORS exposes Content-Length"
    )]
    MissingRangeFallbackLength { href: String },

    #[error("Range fallback for {href} returned invalid Content-Length \"{value}\"")]
    InvalidRangeFallbackLength { href: String, value: String },

    #[error("Failed to read bounded range fallback for {href}: {source}")]
    RangeFallbackBody { href: String, source: HttpError },

    #[error("Invalid fallback range for {href}: {source}")]
    InvalidRangeFallback {
        href: String,
        source: crate::util::InvalidGetRange,
    },

    #[error("Error decoding PROPFIND response: {}", source)]
    InvalidPropFind { source: quick_xml::de::DeError },

    #[error("Missing content size for {}", href)]
    MissingSize { href: String },

    #[error("Error getting properties of \"{}\" got \"{}\"", href, status)]
    PropStatus { href: String, status: String },

    #[error("Failed to parse href \"{}\": {}", href, source)]
    InvalidHref {
        href: String,
        source: url::ParseError,
    },

    #[error("Path \"{}\" contained non-unicode characters: {}", path, source)]
    NonUnicode {
        path: String,
        source: std::str::Utf8Error,
    },

    #[error("Encountered invalid path \"{}\": {}", path, source)]
    InvalidPath {
        path: String,
        source: crate::path::Error,
    },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Request { source, path } => source.error(STORE, path),
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

/// Internal client for HttpStore
#[derive(Debug)]
pub(crate) struct Client {
    url: Url,
    client: HttpClient,
    retry_config: RetryConfig,
    client_options: ClientOptions,
    max_full_object_fallback_size: Option<u64>,
}

impl Client {
    pub(crate) fn new(
        url: Url,
        client: HttpClient,
        client_options: ClientOptions,
        retry_config: RetryConfig,
        max_full_object_fallback_size: Option<u64>,
    ) -> Self {
        Self {
            url,
            retry_config,
            client_options,
            client,
            max_full_object_fallback_size,
        }
    }

    pub(crate) fn base_url(&self) -> &Url {
        &self.url
    }

    fn path_url(&self, location: &Path) -> String {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().extend(location.parts());
        url.to_string()
    }

    async fn normalize_range_response(
        &self,
        path: &Path,
        requested: GetRange,
        if_range: Option<String>,
        response: HttpResponse,
    ) -> Result<HttpResponse> {
        let href = path.to_string();
        let status = response.status();
        let actual_etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);

        if let Some(expected) = if_range {
            if status != StatusCode::PARTIAL_CONTENT
                || actual_etag.as_deref() != Some(expected.as_str())
            {
                return Err(Error::RangeValidatorChanged {
                    href,
                    expected,
                    actual: actual_etag,
                }
                .into());
            }
        }

        if status == StatusCode::PARTIAL_CONTENT {
            return Ok(response);
        }

        if status != StatusCode::OK {
            return Err(crate::Error::NotSupported {
                source: Box::new(Error::RangeNotSupported { href }),
            });
        }

        let Some(max_size) = self.max_full_object_fallback_size else {
            return Err(crate::Error::NotSupported {
                source: Box::new(Error::RangeNotSupported { href }),
            });
        };

        validate_identity_encoding(response.headers()).map_err(|source| crate::Error::Generic {
            store: STORE,
            source: Box::new(source),
        })?;

        let content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .ok_or_else(|| Error::MissingRangeFallbackLength { href: href.clone() })?
            .to_str()
            .map_err(|_| Error::InvalidRangeFallbackLength {
                href: href.clone(),
                value: "<non-ASCII>".into(),
            })?;
        let declared = content_length
            .parse()
            .map_err(|_| Error::InvalidRangeFallbackLength {
                href: href.clone(),
                value: content_length.into(),
            })?;

        if declared > max_size || declared > usize::MAX as u64 {
            return Err(Error::RangeFallbackTooLarge {
                href,
                object_size: declared,
                max_size,
            }
            .into());
        }

        let range = requested
            .as_range(declared)
            .map_err(|source| Error::InvalidRangeFallback {
                href: href.clone(),
                source,
            })?;
        let (mut parts, mut body) = response.into_parts();
        let mut bytes = BytesMut::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|source| Error::RangeFallbackBody {
                href: href.clone(),
                source,
            })?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            let actual = bytes.len() as u64 + data.len() as u64;
            if actual > max_size {
                return Err(Error::RangeFallbackTooLarge {
                    href,
                    object_size: actual,
                    max_size,
                }
                .into());
            }
            bytes.extend_from_slice(&data);
        }

        let actual = bytes.len() as u64;
        if actual != declared {
            return Err(Error::RangeFallbackLength {
                href,
                declared,
                actual,
            }
            .into());
        }

        let start = range.start as usize;
        let end = range.end as usize;
        let selected = bytes.freeze().slice(start..end);
        parts.status = StatusCode::PARTIAL_CONTENT;
        parts.headers.insert(CONTENT_LENGTH, selected.len().into());
        parts.headers.insert(
            CONTENT_RANGE,
            format!("bytes {}-{}/{}", range.start, range.end - 1, declared)
                .parse()
                .expect("generated Content-Range must be valid"),
        );
        Ok(HttpResponse::from_parts(parts, selected.into()))
    }

    /// Create a directory with `path` using MKCOL
    async fn make_directory(&self, path: &str) -> Result<(), Error> {
        let method = Method::from_bytes(b"MKCOL").unwrap();
        let mut url = self.url.clone();
        url.path_segments_mut()
            .unwrap()
            .extend(path.split(DELIMITER));

        self.client
            .request(method, String::from(url))
            .send_retry(&self.retry_config)
            .await
            .map_err(|source| Error::Request {
                source,
                path: path.to_string(),
            })?;

        Ok(())
    }

    /// Recursively create parent directories
    async fn create_parent_directories(&self, location: &Path) -> Result<()> {
        let mut stack = vec![];

        // Walk backwards until a request succeeds
        let mut last_prefix = location.as_ref();
        while let Some((prefix, _)) = last_prefix.rsplit_once(DELIMITER) {
            last_prefix = prefix;

            match self.make_directory(prefix).await {
                Ok(_) => break,
                Err(Error::Request { source, path: _ })
                    if matches!(source.status(), Some(StatusCode::CONFLICT)) =>
                {
                    // Need to create parent
                    stack.push(prefix)
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Retry the failed requests, which should now succeed
        for prefix in stack.into_iter().rev() {
            self.make_directory(prefix).await?;
        }

        Ok(())
    }

    pub(crate) async fn put(
        &self,
        location: &Path,
        payload: PutPayload,
        attributes: Attributes,
    ) -> Result<HttpResponse> {
        let mut retry = false;
        loop {
            let url = self.path_url(location);
            let mut builder = self.client.put(url);

            let mut has_content_type = false;
            for (k, v) in &attributes {
                builder = match k {
                    Attribute::CacheControl => builder.header(CACHE_CONTROL, v.as_ref()),
                    Attribute::ContentDisposition => {
                        builder.header(CONTENT_DISPOSITION, v.as_ref())
                    }
                    Attribute::ContentEncoding => builder.header(CONTENT_ENCODING, v.as_ref()),
                    Attribute::ContentLanguage => builder.header(CONTENT_LANGUAGE, v.as_ref()),
                    Attribute::ContentType => {
                        has_content_type = true;
                        builder.header(CONTENT_TYPE, v.as_ref())
                    }
                    Attribute::StorageClass => {
                        tracing::warn!(
                            "StorageClass attribute not supported on HTTP client as header key is unknown"
                        );
                        builder
                    }
                    // Ignore metadata attributes
                    Attribute::Metadata(_) => builder,
                };
            }

            if !has_content_type {
                if let Some(value) = self.client_options.get_content_type(location) {
                    builder = builder.header(CONTENT_TYPE, value);
                }
            }

            let resp = builder
                .header(CONTENT_LENGTH, payload.content_length())
                .retryable(&self.retry_config)
                .idempotent(true)
                .payload(Some(payload.clone()))
                .send()
                .await;

            match resp {
                Ok(response) => return Ok(response),
                Err(source) => match source.status() {
                    // Some implementations return 404 instead of 409
                    Some(StatusCode::CONFLICT | StatusCode::NOT_FOUND) if !retry => {
                        retry = true;
                        self.create_parent_directories(location).await?
                    }
                    _ => {
                        return Err(Error::Request {
                            source,
                            path: location.to_string(),
                        }
                        .into());
                    }
                },
            }
        }
    }

    pub(crate) async fn list(&self, location: Option<&Path>, depth: &str) -> Result<MultiStatus> {
        let url = location
            .map(|path| self.path_url(path))
            .unwrap_or_else(|| self.url.to_string());

        let method = Method::from_bytes(b"PROPFIND").unwrap();
        let result = self
            .client
            .request(method, url)
            .header("Depth", depth)
            .retryable(&self.retry_config)
            .idempotent(true)
            .send()
            .await;

        let response = match result {
            Ok(result) => result
                .into_body()
                .bytes()
                .await
                .map_err(|source| Error::Reqwest { source })?,
            Err(e) if matches!(e.status(), Some(StatusCode::NOT_FOUND)) => {
                return match depth {
                    "0" => {
                        let path = location.map(|x| x.as_ref()).unwrap_or("");
                        Err(crate::Error::NotFound {
                            path: path.to_string(),
                            source: Box::new(e),
                        })
                    }
                    _ => {
                        // If prefix not found, return empty result set
                        Ok(Default::default())
                    }
                };
            }
            Err(source) => {
                return Err(Error::Request {
                    source,
                    path: location.map(|x| x.to_string()).unwrap_or_default(),
                }
                .into());
            }
        };

        let status = quick_xml::de::from_reader(response.reader())
            .map_err(|source| Error::InvalidPropFind { source })?;

        Ok(status)
    }

    pub(crate) async fn delete(&self, path: &Path) -> Result<()> {
        let url = self.path_url(path);
        self.client
            .delete(url)
            .send_retry(&self.retry_config)
            .await
            .map_err(|source| source.error(STORE, path.to_string()))?;
        Ok(())
    }

    pub(crate) async fn copy(&self, from: &Path, to: &Path, overwrite: bool) -> Result<()> {
        let mut retry = false;
        loop {
            let method = Method::from_bytes(b"COPY").unwrap();

            let mut builder = self
                .client
                .request(method, self.path_url(from))
                .header("Destination", self.path_url(to).as_str());

            if !overwrite {
                // While the Overwrite header appears to duplicate
                // the functionality of the If-Match: * header of HTTP/1.1, If-Match
                // applies only to the Request-URI, and not to the Destination of a COPY
                // or MOVE.
                builder = builder.header("Overwrite", "F");
            }

            return match builder.send_retry(&self.retry_config).await {
                Ok(_) => Ok(()),
                Err(source) => Err(match source.status() {
                    Some(StatusCode::PRECONDITION_FAILED) if !overwrite => {
                        crate::Error::AlreadyExists {
                            path: to.to_string(),
                            source: Box::new(source),
                        }
                    }
                    // Some implementations return 404 instead of 409
                    Some(StatusCode::CONFLICT | StatusCode::NOT_FOUND) if !retry => {
                        retry = true;
                        self.create_parent_directories(to).await?;
                        continue;
                    }
                    _ => Error::Request {
                        source,
                        path: from.to_string(),
                    }
                    .into(),
                }),
            };
        }
    }
}

#[async_trait]
impl GetClient for Client {
    const STORE: &'static str = STORE;

    /// Override the [`HeaderConfig`] to be less strict to support a
    /// broader range of HTTP servers (#4831)
    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: false,
        last_modified_required: false,
        version_header: None,
        user_defined_metadata_prefix: None,
    };

    fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    async fn get_request(
        &self,
        ctx: &mut RetryContext,
        path: &Path,
        options: GetOptions,
    ) -> Result<HttpResponse> {
        let url = self.path_url(path);
        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };
        let range = options.range.clone();
        let if_range = options
            .extensions
            .get::<IfRange>()
            .map(|value| value.0.clone());
        let builder = self.client.request(method, url);
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let builder = match &range {
            Some(_) => builder.header(ACCEPT_ENCODING, "identity"),
            None => builder,
        };

        let res = builder
            .with_get_options(options)
            .retryable_request()
            .send(ctx)
            .await
            .map_err(|source| match source.status() {
                // Some stores return METHOD_NOT_ALLOWED for get on directories
                Some(StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED) => {
                    crate::Error::NotFound {
                        source: Box::new(source),
                        path: path.to_string(),
                    }
                }
                _ => Error::Request {
                    source,
                    path: path.to_string(),
                }
                .into(),
            })?;

        if let Some(range) = range {
            return self
                .normalize_range_response(path, range, if_range, res)
                .await;
        }

        Ok(res)
    }
}

/// The response returned by a PROPFIND request, i.e. list
#[derive(Deserialize, Default)]
pub(crate) struct MultiStatus {
    pub response: Vec<MultiStatusResponse>,
}

#[derive(Deserialize)]
pub(crate) struct MultiStatusResponse {
    href: String,
    #[serde(rename = "propstat")]
    prop_stat: PropStat,
}

impl MultiStatusResponse {
    /// Returns an error if this response is not OK
    pub(crate) fn check_ok(&self) -> Result<()> {
        match self.prop_stat.status.contains("200 OK") {
            true => Ok(()),
            false => Err(Error::PropStatus {
                href: self.href.clone(),
                status: self.prop_stat.status.clone(),
            }
            .into()),
        }
    }

    /// Returns the resolved path of this element relative to `base_url`
    pub(crate) fn path(&self, base_url: &Url) -> Result<Path> {
        let url = Url::options()
            .base_url(Some(base_url))
            .parse(&self.href)
            .map_err(|source| Error::InvalidHref {
                href: self.href.clone(),
                source,
            })?;

        // Reverse any percent encoding
        let path = percent_decode_str(url.path())
            .decode_utf8()
            .map_err(|source| Error::NonUnicode {
                path: url.path().into(),
                source,
            })?;

        Ok(Path::parse(path.as_ref()).map_err(|source| {
            let path = path.into();
            Error::InvalidPath { path, source }
        })?)
    }

    fn size(&self) -> Result<u64> {
        let size = self
            .prop_stat
            .prop
            .content_length
            .ok_or_else(|| Error::MissingSize {
                href: self.href.clone(),
            })?;

        Ok(size)
    }

    /// Returns this objects metadata as [`ObjectMeta`]
    pub(crate) fn object_meta(&self, base_url: &Url) -> Result<ObjectMeta> {
        let last_modified = self.prop_stat.prop.last_modified;
        Ok(ObjectMeta {
            location: self.path(base_url)?,
            last_modified,
            size: self.size()?,
            e_tag: self.prop_stat.prop.e_tag.clone(),
            version: None,
        })
    }

    /// Returns true if this is a directory / collection
    pub(crate) fn is_dir(&self) -> bool {
        self.prop_stat.prop.resource_type.collection.is_some()
    }
}

#[derive(Deserialize)]
pub(crate) struct PropStat {
    prop: Prop,
    status: String,
}

#[derive(Deserialize)]
pub(crate) struct Prop {
    #[serde(deserialize_with = "deserialize_rfc1123", rename = "getlastmodified")]
    last_modified: DateTime<Utc>,

    #[serde(rename = "getcontentlength")]
    content_length: Option<u64>,

    #[serde(rename = "resourcetype")]
    resource_type: ResourceType,

    #[serde(rename = "getetag")]
    e_tag: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct ResourceType {
    collection: Option<()>,
}

#[cfg(all(test, feature = "http", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    fn client(max_size: u64) -> Client {
        Client::new(
            Url::parse("http://example.invalid/").unwrap(),
            HttpClient::new(reqwest::Client::new()),
            ClientOptions::new().with_allow_http(true),
            RetryConfig::default(),
            Some(max_size),
        )
    }

    #[tokio::test]
    async fn bounded_fallback_stops_before_unbounded_buffering() {
        let response = http::Response::builder()
            .header(CONTENT_LENGTH, 3)
            .body("12345".to_string().into())
            .unwrap();

        let error = client(4)
            .normalize_range_response(&Path::from("test"), GetRange::Bounded(0..2), None, response)
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("5-byte object exceeds configured 4-byte"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn bounded_fallback_rejects_actual_length_mismatch() {
        let response = http::Response::builder()
            .header(CONTENT_LENGTH, 3)
            .body("12".to_string().into())
            .unwrap();

        let error = client(4)
            .normalize_range_response(&Path::from("test"), GetRange::Bounded(0..2), None, response)
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("declared 3 bytes but returned 2 bytes"),
            "{message}"
        );
    }
}
