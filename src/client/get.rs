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

use crate::client::header::{HeaderConfig, get_etag, header_meta};
use crate::client::retry::RetryContext;
use crate::client::{HttpError, HttpErrorKind, HttpResponse, HttpResponseBody};
use crate::path::Path;
use crate::{
    Attribute, Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ObjectMeta, Result,
    RetryConfig,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use http::StatusCode;
use http::header::ToStrError;
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE,
};
use http_body_util::BodyExt;
use std::ops::Range;
use std::sync::Arc;
use tracing::info;

/// A client that can perform a get request
#[async_trait]
pub(crate) trait GetClient: Send + Sync + 'static {
    const STORE: &'static str;

    /// Configure the [`HeaderConfig`] for this client
    const HEADER_CONFIG: HeaderConfig;

    fn retry_config(&self) -> &RetryConfig;

    async fn get_request(
        &self,
        ctx: &mut RetryContext,
        path: &Path,
        options: GetOptions,
    ) -> Result<HttpResponse>;
}

/// Extension trait for [`GetClient`] that adds common retrieval functionality
#[async_trait]
pub(crate) trait GetClientExt {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult>;
}

#[async_trait]
impl<T: GetClient> GetClientExt for Arc<T> {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let ctx = GetContext {
            location: location.clone(),
            options,
            client: Self::clone(self),
            retry_ctx: RetryContext::new(self.retry_config()),
        };

        ctx.get_result().await
    }
}

#[derive(Debug, Clone)]
struct ContentRange {
    /// The range of the object returned
    range: Range<u64>,
    /// The total size of the object being requested
    size: u64,
}

impl ContentRange {
    /// Parse a content range of the form `bytes <range-start>-<range-end>/<size>`
    ///
    /// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
    fn from_str(s: &str) -> Option<Self> {
        let rem = s.trim().strip_prefix("bytes ")?;
        let (range, size) = rem.split_once('/')?;
        let size = size.parse().ok()?;

        let (start_s, end_s) = range.split_once('-')?;

        let start = start_s.parse().ok()?;
        let end: u64 = end_s.parse().ok()?;
        let end = end.checked_add(1)?;

        if start >= end || end > size {
            return None;
        }

        Some(Self {
            size,
            range: start..end,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IfRange(pub(crate) String);

/// A specialized `Error` for get-related errors
#[derive(Debug, thiserror::Error)]
pub(crate) enum GetResultError {
    #[error(transparent)]
    Header {
        #[from]
        source: crate::client::header::Error,
    },

    #[error(transparent)]
    InvalidRangeRequest {
        #[from]
        source: crate::util::InvalidGetRange,
    },

    #[error("Received non-partial response when range requested")]
    NotPartial,

    #[error(
        "Content-Range header not present in partial response; for browser requests verify CORS exposes Content-Range"
    )]
    NoContentRange,

    #[error("Failed to parse value for CONTENT_RANGE header: \"{value}\"")]
    ParseContentRange { value: String },

    #[error("Content-Range header contained non UTF-8 characters")]
    InvalidContentRange { source: ToStrError },

    #[error("Cache-Control header contained non UTF-8 characters")]
    InvalidCacheControl { source: ToStrError },

    #[error("Content-Disposition header contained non UTF-8 characters")]
    InvalidContentDisposition { source: ToStrError },

    #[error("Content-Encoding header contained non UTF-8 characters")]
    InvalidContentEncoding { source: ToStrError },

    #[error("Content-Language header contained non UTF-8 characters")]
    InvalidContentLanguage { source: ToStrError },

    #[error("Content-Type header contained non UTF-8 characters")]
    InvalidContentType { source: ToStrError },

    #[error("Metadata value for \"{key:?}\" contained non UTF-8 characters")]
    InvalidMetadata { key: String },

    #[error("Requested {expected:?}, got {actual:?}")]
    UnexpectedRange {
        expected: Range<u64>,
        actual: Range<u64>,
    },

    #[error("Range response used unsupported Content-Encoding \"{encoding}\"; expected identity")]
    EncodedRange { encoding: String },

    #[error("Range response declared {declared} bytes for Content-Range {actual:?}")]
    InvalidRangeLength { declared: u64, actual: Range<u64> },

    #[error("Response body ended at byte {actual}, expected byte {expected}")]
    TruncatedBody { expected: u64, actual: u64 },

    #[error("Response body exceeded expected end byte {expected}")]
    BodyTooLong { expected: u64 },
}

/// Retry context for a streaming get request
struct GetContext<T: GetClient> {
    client: Arc<T>,
    location: Path,
    options: GetOptions,
    retry_ctx: RetryContext,
}

impl<T: GetClient> GetContext<T> {
    async fn get_result(mut self) -> Result<GetResult> {
        if let Some(r) = &self.options.range {
            r.is_valid().map_err(Self::err)?;
        }

        let request = self
            .client
            .get_request(&mut self.retry_ctx, &self.location, self.options.clone())
            .await?;

        let (parts, body) = request.into_parts();
        let (range, meta) = get_range_meta(
            T::HEADER_CONFIG,
            &self.location,
            self.options.range.as_ref(),
            &parts,
        )
        .map_err(Self::err)?;

        let attributes = get_attributes(T::HEADER_CONFIG, &parts.headers).map_err(Self::err)?;
        let stream = self.retry_stream(body, meta.e_tag.clone(), range.clone());

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes,
            extensions: parts.extensions,
        })
    }

    fn retry_stream(
        self,
        body: HttpResponseBody,
        etag: Option<String>,
        range: Range<u64>,
    ) -> BoxStream<'static, Result<Bytes>> {
        // A partial response can only be safely combined with bytes already delivered when the
        // representation is protected by a strong validator (RFC 9110 section 8.8.3).
        let etag = etag.filter(|etag| is_strong_etag(etag));
        futures_util::stream::try_unfold(
            (self, body, etag, range),
            |(mut ctx, mut body, etag, mut range)| async move {
                loop {
                    let ret = match body.frame().await {
                        Some(ret) => ret,
                        None if range.start == range.end => return Ok(None),
                        None if etag.is_some() && !ctx.retry_ctx.exhausted() => {
                            Err(HttpError::new(
                                HttpErrorKind::Interrupted,
                                GetResultError::TruncatedBody {
                                    expected: range.end,
                                    actual: range.start,
                                },
                            ))
                        }
                        None => {
                            return Err(Self::err(GetResultError::TruncatedBody {
                                expected: range.end,
                                actual: range.start,
                            }));
                        }
                    };
                    match (ret, &etag) {
                        (Ok(frame), _) => match frame.into_data() {
                            Ok(bytes) => {
                                let next = range
                                    .start
                                    .checked_add(bytes.len() as u64)
                                    .ok_or_else(|| {
                                        Self::err(GetResultError::BodyTooLong {
                                            expected: range.end,
                                        })
                                    })?;
                                if next > range.end {
                                    return Err(Self::err(GetResultError::BodyTooLong {
                                        expected: range.end,
                                    }));
                                }
                                range.start = next;
                                return Ok(Some((bytes, (ctx, body, etag, range))));
                            }
                            Err(_) => continue, // Isn't data frame
                        },
                        // Retry all response body errors
                        (Err(e), Some(etag)) if !ctx.retry_ctx.exhausted() => {
                            let sleep = ctx.retry_ctx.backoff();
                            info!(
                                "Encountered error while reading response body: {}. Retrying in {}s",
                                e,
                                sleep.as_secs_f32()
                            );

                            ctx.retry_ctx.sleep(sleep).await.map_err(Self::err)?;

                            let mut options = GetOptions {
                                range: Some(GetRange::Bounded(range.clone())),
                                ..ctx.options.clone()
                            };
                            if is_strong_etag(etag) {
                                options.extensions.insert(IfRange(etag.clone()));
                            }

                            // Note: this will potentially retry internally if applicable
                            let request = ctx
                                .client
                                .get_request(&mut ctx.retry_ctx, &ctx.location, options)
                                .await
                                .map_err(Self::err)?;

                            let (parts, retry_body) = request.into_parts();
                            let retry_etag = get_etag(&parts.headers).map_err(Self::err)?;

                            if etag != &retry_etag {
                                // Return the original error
                                return Err(Self::err(e));
                            }

                            let content_range =
                                validate_range_response(&parts.headers).map_err(Self::err)?;
                            let actual = content_range.range;

                            if actual == range {
                                body = retry_body;
                            } else if actual.start <= range.start && actual.end >= range.end {
                                let skip = (range.start - actual.start) as usize;
                                let mut skipped = 0;
                                let mut retry_body = retry_body;
                                while skipped < skip {
                                    let frame = retry_body
                                        .frame()
                                        .await
                                        .ok_or_else(|| {
                                            Self::err(GetResultError::UnexpectedRange {
                                                expected: range.clone(),
                                                actual: actual.clone(),
                                            })
                                        })?
                                        .map_err(Self::err)?;
                                    let Some(bytes) = frame.into_data().ok() else {
                                        continue;
                                    };
                                    let remaining = skip - skipped;
                                    if bytes.len() <= remaining {
                                        skipped += bytes.len();
                                    } else {
                                        let keep = bytes.slice(remaining..);
                                        let next = range.start + keep.len() as u64;
                                        if next > range.end {
                                            return Err(Self::err(GetResultError::BodyTooLong {
                                                expected: range.end,
                                            }));
                                        }
                                        range.start = next;
                                        body = retry_body;
                                        let etag = Some(etag.clone());
                                        return Ok(Some((keep, (ctx, body, etag, range))));
                                    }
                                }
                                body = retry_body;
                            } else {
                                return Err(Self::err(GetResultError::UnexpectedRange {
                                    expected: range,
                                    actual,
                                }));
                            }
                        }
                        (Err(e), _) => return Err(Self::err(e)),
                    }
                }
            },
        )
            .boxed()
    }

    fn err<E: std::error::Error + Send + Sync + 'static>(e: E) -> crate::Error {
        crate::Error::Generic {
            store: T::STORE,
            source: Box::new(e),
        }
    }
}

fn get_range_meta(
    cfg: HeaderConfig,
    location: &Path,
    range: Option<&GetRange>,
    response: &http::response::Parts,
) -> Result<(Range<u64>, ObjectMeta), GetResultError> {
    let mut meta = header_meta(location, &response.headers, cfg)?;
    let range = if let Some(expected) = range {
        if response.status != StatusCode::PARTIAL_CONTENT {
            return Err(GetResultError::NotPartial);
        }

        let value = validate_range_response(&response.headers)?;
        let actual = value.range;

        // Update size to reflect the full size of the object (#5272)
        meta.size = value.size;

        let expected = expected.as_range(meta.size)?;
        if actual != expected {
            return Err(GetResultError::UnexpectedRange { expected, actual });
        }

        actual
    } else {
        0..meta.size
    };

    Ok((range, meta))
}

fn validate_range_response(headers: &http::HeaderMap) -> Result<ContentRange, GetResultError> {
    validate_identity_encoding(headers)?;
    let content_range = parse_range(headers)?;
    let content_length = headers
        .get(CONTENT_LENGTH)
        .ok_or(crate::client::header::Error::MissingContentLength)?
        .to_str()
        .map_err(|source| crate::client::header::Error::BadHeader { source })?;
    let declared = content_length.parse().map_err(|source| {
        crate::client::header::Error::InvalidContentLength {
            content_length: content_length.into(),
            source,
        }
    })?;
    let actual_length = content_range.range.end - content_range.range.start;
    if declared != actual_length {
        return Err(GetResultError::InvalidRangeLength {
            declared,
            actual: content_range.range,
        });
    }
    Ok(content_range)
}

pub(crate) fn validate_identity_encoding(headers: &http::HeaderMap) -> Result<(), GetResultError> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(());
    };
    let encoding = value
        .to_str()
        .map_err(|source| GetResultError::InvalidContentEncoding { source })?;
    if !encoding.trim().eq_ignore_ascii_case("identity") {
        return Err(GetResultError::EncodedRange {
            encoding: encoding.into(),
        });
    }
    Ok(())
}

fn is_strong_etag(etag: &str) -> bool {
    let bytes = etag.trim().as_bytes();
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return false;
    }

    bytes[1..bytes.len() - 1]
        .iter()
        .all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte) || *byte >= 0x80)
}

/// Extracts the [CONTENT_RANGE] header
fn parse_range(headers: &http::HeaderMap) -> Result<ContentRange, GetResultError> {
    let val = headers
        .get(CONTENT_RANGE)
        .ok_or(GetResultError::NoContentRange)?;

    let value = val
        .to_str()
        .map_err(|source| GetResultError::InvalidContentRange { source })?;

    ContentRange::from_str(value).ok_or_else(|| {
        let value = value.into();
        GetResultError::ParseContentRange { value }
    })
}

/// Extracts [`Attributes`] from the response headers
fn get_attributes(
    cfg: HeaderConfig,
    headers: &http::HeaderMap,
) -> Result<Attributes, GetResultError> {
    macro_rules! parse_attributes {
        ($headers:expr, $(($header:expr, $attr:expr, $map_err:expr)),*) => {{
            let mut attributes = Attributes::new();
            $(
            if let Some(x) = $headers.get($header) {
                let x = x.to_str().map_err($map_err)?;
                attributes.insert($attr, x.to_string().into());
            }
            )*
            attributes
        }}
    }

    let mut attributes = parse_attributes!(
        headers,
        (CACHE_CONTROL, Attribute::CacheControl, |source| {
            GetResultError::InvalidCacheControl { source }
        }),
        (
            CONTENT_DISPOSITION,
            Attribute::ContentDisposition,
            |source| GetResultError::InvalidContentDisposition { source }
        ),
        (CONTENT_ENCODING, Attribute::ContentEncoding, |source| {
            GetResultError::InvalidContentEncoding { source }
        }),
        (CONTENT_LANGUAGE, Attribute::ContentLanguage, |source| {
            GetResultError::InvalidContentLanguage { source }
        }),
        (CONTENT_TYPE, Attribute::ContentType, |source| {
            GetResultError::InvalidContentType { source }
        })
    );

    // Add attributes that match the user-defined metadata prefix (e.g. x-amz-meta-)
    if let Some(prefix) = cfg.user_defined_metadata_prefix {
        for (key, val) in headers {
            if let Some(suffix) = key.as_str().strip_prefix(prefix) {
                if let Ok(val_str) = val.to_str() {
                    attributes.insert(
                        Attribute::Metadata(suffix.to_string().into()),
                        val_str.to_string().into(),
                    );
                } else {
                    return Err(GetResultError::InvalidMetadata {
                        key: key.to_string(),
                    });
                }
            }
        }
    }
    Ok(attributes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_response(
        object_size: usize,
        status: StatusCode,
        content_range: Option<&str>,
        headers: Option<Vec<(&str, &str)>>,
    ) -> http::response::Parts {
        let mut builder = http::Response::builder();
        if let Some(range) = content_range {
            builder = builder.header(CONTENT_RANGE, range);
        }

        if let Some(headers) = headers {
            for (key, value) in headers {
                builder = builder.header(key, value);
            }
        }

        builder
            .status(status)
            .header(CONTENT_LENGTH, object_size)
            .body(())
            .unwrap()
            .into_parts()
            .0
    }

    const CFG: HeaderConfig = HeaderConfig {
        etag_required: false,
        last_modified_required: false,
        version_header: None,
        user_defined_metadata_prefix: Some("x-test-meta-"),
    };

    #[derive(Debug)]
    struct CleanEofClient {
        requests: AtomicUsize,
        retry: RetryConfig,
    }

    #[derive(Debug)]
    struct NonStrongEofClient {
        requests: AtomicUsize,
        retry: RetryConfig,
        etag: Option<&'static str>,
    }

    #[async_trait]
    impl GetClient for CleanEofClient {
        const STORE: &'static str = "clean EOF";
        const HEADER_CONFIG: HeaderConfig = CFG;

        fn retry_config(&self) -> &RetryConfig {
            &self.retry
        }

        async fn get_request(
            &self,
            _ctx: &mut RetryContext,
            _path: &Path,
            options: GetOptions,
        ) -> Result<HttpResponse> {
            let request = self.requests.fetch_add(1, Ordering::Relaxed);
            let response = match request {
                0 => {
                    assert!(options.range.is_none());
                    http::Response::builder()
                        .header(CONTENT_LENGTH, 10)
                        .header(http::header::ETAG, "\"abc\"")
                        .body(HttpResponseBody::from(Bytes::from_static(b"hello")))
                        .unwrap()
                }
                1 => {
                    assert_eq!(options.range, Some(GetRange::Bounded(5..10)));
                    assert_eq!(
                        options
                            .extensions
                            .get::<IfRange>()
                            .map(|value| value.0.as_str()),
                        Some("\"abc\"")
                    );
                    http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(CONTENT_LENGTH, 5)
                        .header(CONTENT_RANGE, "bytes 5-9/10")
                        .header(http::header::ETAG, "\"abc\"")
                        .body(HttpResponseBody::from(Bytes::from_static(b"world")))
                        .unwrap()
                }
                _ => panic!("unexpected clean-EOF retry request {request}"),
            };
            Ok(response)
        }
    }

    #[async_trait]
    impl GetClient for NonStrongEofClient {
        const STORE: &'static str = "non-strong EOF";
        const HEADER_CONFIG: HeaderConfig = CFG;

        fn retry_config(&self) -> &RetryConfig {
            &self.retry
        }

        async fn get_request(
            &self,
            _ctx: &mut RetryContext,
            _path: &Path,
            options: GetOptions,
        ) -> Result<HttpResponse> {
            let request = self.requests.fetch_add(1, Ordering::Relaxed);
            assert_eq!(
                request, 0,
                "a truncated body without a strong ETag must not be retried"
            );
            assert!(options.range.is_none());

            let mut response = http::Response::builder().header(CONTENT_LENGTH, 10);
            if let Some(etag) = self.etag {
                response = response.header(http::header::ETAG, etag);
            }
            Ok(response
                .body(HttpResponseBody::from(Bytes::from_static(b"hello")))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn retries_a_clean_eof_before_the_declared_body_length() {
        let client = Arc::new(CleanEofClient {
            requests: AtomicUsize::new(0),
            retry: RetryConfig {
                max_retries: 3,
                ..Default::default()
            },
        });

        let result = client
            .get_opts(&Path::from("test"), GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(result.as_ref(), b"helloworld");
        assert_eq!(client.requests.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn truncated_body_is_not_retried_without_a_valid_strong_etag() {
        for etag in [
            None,
            Some("W/\"abc\""),
            Some("abc"),
            Some("\"abc\",\"other\""),
        ] {
            let client = Arc::new(NonStrongEofClient {
                requests: AtomicUsize::new(0),
                retry: RetryConfig {
                    max_retries: 3,
                    ..Default::default()
                },
                etag,
            });

            let error = client
                .get_opts(&Path::from("test"), GetOptions::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("Response body ended at byte 5, expected byte 10"),
                "unexpected error for ETag {etag:?}: {error}"
            );
            assert_eq!(client.requests.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn strong_etag_validation_follows_entity_tag_grammar() {
        assert!(is_strong_etag("\"\""));
        assert!(is_strong_etag("\"abc\""));
        assert!(!is_strong_etag("W/\"abc\""));
        assert!(!is_strong_etag("abc"));
        assert!(!is_strong_etag("\"abc"));
        assert!(!is_strong_etag("\"abc\",\"other\""));
        assert!(!is_strong_etag("\"abc def\""));
    }

    #[tokio::test]
    async fn test_get_range_meta() {
        let path = Path::from("test");

        let resp = make_response(12, StatusCode::OK, None, None);
        let (range, meta) = get_range_meta(CFG, &path, None, &resp).unwrap();
        assert_eq!(meta.size, 12);
        assert_eq!(range, 0..12);

        let get_range = GetRange::from(2..3);

        let resp = make_response(1, StatusCode::PARTIAL_CONTENT, Some("bytes 2-2/12"), None);
        let (range, meta) = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap();
        assert_eq!(meta.size, 12);
        assert_eq!(range, 2..3);

        let resp = make_response(12, StatusCode::OK, None, None);
        let err = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Received non-partial response when range requested"
        );

        let resp = make_response(2, StatusCode::PARTIAL_CONTENT, Some("bytes 2-3/12"), None);
        let err = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap_err();
        assert_eq!(err.to_string(), "Requested 2..3, got 2..4");

        let resp = make_response(12, StatusCode::PARTIAL_CONTENT, Some("bytes 2-2/*"), None);
        let err = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Failed to parse value for CONTENT_RANGE header: \"bytes 2-2/*\""
        );

        let resp = make_response(12, StatusCode::PARTIAL_CONTENT, None, None);
        let err = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Content-Range header not present in partial response; for browser requests verify CORS exposes Content-Range"
        );

        let resp = make_response(2, StatusCode::PARTIAL_CONTENT, Some("bytes 0-1/2"), None);
        let err = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Wanted range starting at 2, but object was only 2 bytes long"
        );

        let resp = make_response(4, StatusCode::PARTIAL_CONTENT, Some("bytes 2-5/6"), None);
        let (range, meta) = get_range_meta(CFG, &path, Some(&GetRange::Suffix(4)), &resp).unwrap();
        assert_eq!(meta.size, 6);
        assert_eq!(range, 2..6);

        let resp = make_response(2, StatusCode::PARTIAL_CONTENT, Some("bytes 2-3/6"), None);
        let err = get_range_meta(CFG, &path, Some(&GetRange::Suffix(4)), &resp).unwrap_err();
        assert_eq!(err.to_string(), "Requested 2..6, got 2..4");
    }

    #[test]
    fn test_get_range_meta_rejects_encoded_body() {
        let path = Path::from("test");
        let get_range = GetRange::from(2..5);
        let resp = make_response(
            3,
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 2-4/12"),
            Some(vec![("content-encoding", "gzip")]),
        );

        let err = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Range response used unsupported Content-Encoding \"gzip\"; expected identity"
        );
    }

    #[test]
    fn test_get_range_meta_rejects_invalid_lengths() {
        let path = Path::from("test");
        let get_range = GetRange::from(2..5);
        let resp = make_response(2, StatusCode::PARTIAL_CONTENT, Some("bytes 2-4/12"), None);

        let err = get_range_meta(CFG, &path, Some(&get_range), &resp).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Range response declared 2 bytes for Content-Range 2..5"
        );

        assert!(ContentRange::from_str("bytes 4-2/12").is_none());
        assert!(ContentRange::from_str("bytes 2-12/12").is_none());
        assert!(
            ContentRange::from_str("bytes 2-18446744073709551615/18446744073709551615").is_none()
        );
    }

    #[test]
    fn test_get_attributes() {
        let resp = make_response(
            12,
            StatusCode::OK,
            None,
            Some(vec![("x-test-meta-foo", "bar")]),
        );

        let attributes = get_attributes(CFG, &resp.headers).unwrap();
        assert_eq!(
            attributes.get(&Attribute::Metadata("foo".into())),
            Some(&"bar".into())
        );
    }
}
#[cfg(all(test, feature = "http-base", not(target_arch = "wasm32")))]
mod http_tests {
    use crate::client::mock_server::MockServer;
    use crate::client::{HttpError, HttpErrorKind, HttpResponseBody};
    use crate::http::HttpBuilder;
    use crate::path::Path;
    use crate::{ClientOptions, ObjectStoreExt, RetryConfig};
    use bytes::Bytes;
    use futures_util::FutureExt;
    use http::header::{
        ACCEPT_ENCODING, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG,
        IF_RANGE, RANGE,
    };
    use http::{Response, StatusCode};
    use hyper::body::Frame;
    use std::pin::Pin;
    use std::task::{Context, Poll, ready};
    use std::time::Duration;

    #[derive(Debug, thiserror::Error)]
    #[error("ChunkedErr")]
    struct ChunkedErr {}

    /// A Body from a list of results
    ///
    /// Sleeps between each frame to avoid the HTTP Server coalescing the frames
    struct Chunked {
        chunks: std::vec::IntoIter<Result<Bytes, ()>>,
        sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    }

    impl Chunked {
        fn new(v: Vec<Result<Bytes, ()>>) -> Self {
            Self {
                chunks: v.into_iter(),
                sleep: None,
            }
        }
    }

    impl hyper::body::Body for Chunked {
        type Data = Bytes;
        type Error = HttpError;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if let Some(sleep) = &mut self.sleep {
                ready!(sleep.poll_unpin(cx));
                self.sleep = None;
            }

            Poll::Ready(match self.chunks.next() {
                None => None,
                Some(Ok(b)) => {
                    self.sleep = Some(Box::pin(tokio::time::sleep(Duration::from_millis(1))));
                    Some(Ok(Frame::data(b)))
                }
                Some(Err(_)) => Some(Err(HttpError::new(HttpErrorKind::Unknown, ChunkedErr {}))),
            })
        }
    }

    impl From<Chunked> for HttpResponseBody {
        fn from(value: Chunked) -> Self {
            Self::new(value)
        }
    }

    #[cfg(feature = "reqwest")]
    #[tokio::test]
    async fn test_stream_retry() {
        let mock = MockServer::new().await;
        let retry = RetryConfig {
            backoff: Default::default(),
            max_retries: 3,
            retry_timeout: Duration::from_secs(1000),
        };

        let options = ClientOptions::new().with_allow_http(true);
        let store = HttpBuilder::new()
            .with_client_options(options)
            .with_retry(retry)
            .with_url(mock.url())
            .build()
            .unwrap();

        let path = Path::from("test");

        // Test basic
        let resp = Response::builder()
            .header(CONTENT_LENGTH, 11)
            .header(ETAG, "123")
            .body("Hello World".to_string())
            .unwrap();

        mock.push(resp);

        let b = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(b.as_ref(), b"Hello World");

        // Test basic with range
        let resp = Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(CONTENT_LENGTH, 4)
            .header(ETAG, "123")
            .header(CONTENT_RANGE, "bytes 1-4/11")
            .body("ello".to_string())
            .unwrap();

        mock.push(resp);

        let b = store.get_range(&path, 1..5).await.unwrap();
        assert_eq!(b.as_ref(), b"ello");

        // NOTE: if debug_assertions is true, hyper panics with response content length header
        // value does not match the length of response body.
        if !cfg!(debug_assertions) {
            // Test retry if a response body is incomplete.
            mock.push(
                Response::builder()
                    .header(CONTENT_LENGTH, 12)
                    .header(ETAG, "\"123\"")
                    // connection: close disables keep alive
                    .header(CONNECTION, "close")
                    .body("Hello".to_string())
                    .unwrap(),
            );

            mock.push_fn(|req| {
                assert_eq!(
                    req.headers().get(RANGE).unwrap().to_str().unwrap(),
                    "bytes=5-11"
                );

                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(CONTENT_LENGTH, 7)
                    .header(ETAG, "\"123\"")
                    .header(CONTENT_RANGE, "bytes 5-11/12")
                    .body(" World!".to_string())
                    .unwrap()
            });

            let ret = store.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(ret.as_ref(), b"Hello World!");

            // Test retry if a get range response body is incomplete.
            mock.push(
                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(CONTENT_LENGTH, 5)
                    .header(CONNECTION, "close")
                    .header(ETAG, "\"123\"")
                    .header(CONTENT_RANGE, "bytes 6-10/12")
                    .body("Wo".to_string())
                    .unwrap(),
            );

            mock.push_fn(|req| {
                assert_eq!(
                    req.headers().get(RANGE).unwrap().to_str().unwrap(),
                    "bytes=8-10"
                );

                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(CONTENT_LENGTH, 3)
                    .header(ETAG, "\"123\"")
                    .header(CONTENT_RANGE, "bytes 8-10/12")
                    .body("rld".to_string())
                    .unwrap()
            });

            let ret = store.get_range(&path, 6..11).await.unwrap();
            assert_eq!(ret.as_ref(), b"World");
        }

        // Should retry with range
        mock.push(
            Response::builder()
                .header(CONTENT_LENGTH, 10)
                .header(ETAG, "\"123\"")
                .body(Chunked::new(vec![
                    Ok(Bytes::from_static(b"banana")),
                    Err(()),
                ]))
                .unwrap(),
        );

        mock.push_fn(|req| {
            assert_eq!(
                req.headers().get(RANGE).unwrap().to_str().unwrap(),
                "bytes=6-9"
            );

            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, 4)
                .header(ETAG, "\"123\"")
                .header(CONTENT_RANGE, "bytes 6-9/10")
                .body("1234".to_string())
                .unwrap()
        });

        let ret = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(ret.as_ref(), b"banana1234");

        // Should retry multiple times
        mock.push(
            Response::builder()
                .header(CONTENT_LENGTH, 20)
                .header(ETAG, "\"foo\"")
                .body(Chunked::new(vec![
                    Ok(Bytes::from_static(b"hello")),
                    Err(()),
                ]))
                .unwrap(),
        );

        mock.push_fn(|req| {
            assert_eq!(
                req.headers().get(RANGE).unwrap().to_str().unwrap(),
                "bytes=5-19"
            );

            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, 15)
                .header(ETAG, "\"foo\"")
                .header(CONTENT_RANGE, "bytes 5-19/20")
                .body(Chunked::new(vec![Ok(Bytes::from_static(b"baz")), Err(())]))
                .unwrap()
        });

        mock.push_fn::<_, String>(|req| {
            assert_eq!(
                req.headers().get(RANGE).unwrap().to_str().unwrap(),
                "bytes=8-19"
            );
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body("ignored".to_string())
                .unwrap()
        });

        mock.push_fn(|req| {
            assert_eq!(
                req.headers().get(RANGE).unwrap().to_str().unwrap(),
                "bytes=8-19"
            );

            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, 12)
                .header(ETAG, "\"foo\"")
                .header(CONTENT_RANGE, "bytes 8-19/20")
                .body("123456789012".to_string())
                .unwrap()
        });

        let ret = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(ret.as_ref(), b"hellobaz123456789012");

        // Should abort if etag doesn't match
        mock.push(
            Response::builder()
                .header(CONTENT_LENGTH, 12)
                .header(ETAG, "\"foo\"")
                .body(Chunked::new(vec![Ok(Bytes::from_static(b"test")), Err(())]))
                .unwrap(),
        );

        mock.push_fn(|req| {
            assert_eq!(
                req.headers().get(RANGE).unwrap().to_str().unwrap(),
                "bytes=4-11"
            );

            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, 7)
                .header(ETAG, "\"baz\"")
                .header(CONTENT_RANGE, "bytes 4-11/12")
                .body("1234567".to_string())
                .unwrap()
        });

        let err = store.get(&path).await.unwrap().bytes().await.unwrap_err();
        let message = err.to_string();
        assert!(message.contains("did not honor If-Range"), "{message}");
        assert!(message.contains("\"foo\""), "{message}");
        assert!(message.contains("baz"), "{message}");
    }
    #[tokio::test]
    async fn test_retry_validates_content_range_and_sends_if_range() {
        let mock = MockServer::new().await;
        let retry = RetryConfig {
            backoff: Default::default(),
            max_retries: 3,
            retry_timeout: Duration::from_secs(1000),
        };

        let options = ClientOptions::new().with_allow_http(true);
        let store = HttpBuilder::new()
            .with_client_options(options)
            .with_retry(retry)
            .with_url(mock.url())
            .build()
            .unwrap();
        let path = Path::from("test");

        mock.push(
            Response::builder()
                .header(CONTENT_LENGTH, 10)
                .header(ETAG, "\"abc\"")
                .body(Chunked::new(vec![
                    Ok(Bytes::from_static(b"hello")),
                    Err(()),
                ]))
                .unwrap(),
        );

        mock.push_fn(|req| {
            assert_eq!(req.headers().get(RANGE).unwrap(), "bytes=5-9");
            assert_eq!(req.headers().get(IF_RANGE).unwrap(), "\"abc\"");
            assert_eq!(req.headers().get(ACCEPT_ENCODING).unwrap(), "identity");

            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, 10)
                .header(ETAG, "\"abc\"")
                .header(CONTENT_RANGE, "bytes 0-9/10")
                .body("helloworld".to_string())
                .unwrap()
        });

        let result = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(result.as_ref(), b"helloworld");
    }

    #[tokio::test]
    async fn test_retry_rejects_changed_if_range_validator() {
        let mock = MockServer::new().await;
        let retry = RetryConfig {
            backoff: Default::default(),
            max_retries: 3,
            retry_timeout: Duration::from_secs(1000),
        };
        let options = ClientOptions::new().with_allow_http(true);
        let store = HttpBuilder::new()
            .with_client_options(options)
            .with_retry(retry)
            .with_url(mock.url())
            .build()
            .unwrap();
        let path = Path::from("test");

        mock.push(
            Response::builder()
                .header(CONTENT_LENGTH, 10)
                .header(ETAG, "\"abc\"")
                .body(Chunked::new(vec![
                    Ok(Bytes::from_static(b"hello")),
                    Err(()),
                ]))
                .unwrap(),
        );
        mock.push_fn(|req| {
            assert_eq!(req.headers().get(IF_RANGE).unwrap(), "\"abc\"");
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, 10)
                .header(ETAG, "\"changed\"")
                .body("helloworld".to_string())
                .unwrap()
        });

        let error = store.get(&path).await.unwrap().bytes().await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("did not honor If-Range"), "{message}");
        assert!(message.contains("changed"), "{message}");
    }

    #[tokio::test]
    async fn test_range_response_requires_identity_encoding() {
        let mock = MockServer::new().await;
        mock.push_fn(|req| {
            assert_eq!(req.headers().get(ACCEPT_ENCODING).unwrap(), "identity");
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_LENGTH, 4)
                .header(CONTENT_RANGE, "bytes 1-4/10")
                .header(CONTENT_ENCODING, "gzip")
                .body("bcde".to_string())
                .unwrap()
        });

        let options = ClientOptions::new().with_allow_http(true);
        let store = HttpBuilder::new()
            .with_client_options(options)
            .with_url(mock.url())
            .build()
            .unwrap();
        let error = store
            .get_range(&Path::from("test"), 1..5)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("expected identity"));
    }
}
