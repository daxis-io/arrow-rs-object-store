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

use object_store::{
    BackoffConfig, ClientOptions, GetOptions, GetRange, ObjectStore, ObjectStoreExt, RetryConfig,
    http::HttpBuilder, path::Path,
};
use std::time::Duration;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

const TEST_URL: &str = "http://127.0.0.1:18080";

fn browser_retry() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_millis(25),
            base: 1.,
        },
        max_retries: 2,
        retry_timeout: Duration::from_secs(30),
    }
}

#[wasm_bindgen_test]
async fn retries_a_transient_fetch_after_a_nonzero_browser_delay() {
    let store = HttpBuilder::new()
        .with_url(TEST_URL)
        .with_client_options(ClientOptions::new().with_allow_http(true))
        .with_retry(browser_retry())
        .build()
        .unwrap();

    let bytes = store
        .get_opts(
            &Path::from("transient"),
            GetOptions::new().with_range(Some(GetRange::Bounded(0..5))),
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(bytes.as_ref(), b"hello");
}

#[wasm_bindgen_test]
async fn resumes_a_truncated_fetch_with_if_range() {
    let store = HttpBuilder::new()
        .with_url(TEST_URL)
        .with_client_options(ClientOptions::new().with_allow_http(true))
        .with_retry(browser_retry())
        .build()
        .unwrap();

    let result = store.get(&Path::from("truncated")).await.unwrap();
    assert_eq!(result.meta.e_tag.as_deref(), Some("\"v1\""));
    let bytes = result.bytes().await.unwrap();

    assert_eq!(bytes.as_ref(), b"helloworld");
}

#[wasm_bindgen_test]
async fn rejects_a_200_response_to_an_if_range_retry() {
    let store = HttpBuilder::new()
        .with_url(TEST_URL)
        .with_client_options(ClientOptions::new().with_allow_http(true))
        .with_retry(browser_retry())
        .build()
        .unwrap();

    let error = store
        .get(&Path::from("retry-200"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("Range request not supported by retry-200"),
        "unexpected retry error: {error}"
    );
}
