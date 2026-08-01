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

#[cfg(feature = "http-base")]
pub fn host_neutral_retry_policy() -> object_store::RetryConfig {
    object_store::RetryConfig::default()
}

#[cfg(all(feature = "http-base", feature = "reqwest", feature = "web"))]
pub fn browser_http_store(url: &str) -> object_store::Result<object_store::http::HttpStore> {
    let connector = object_store::client::ReqwestConnector::default();
    object_store::http::HttpBuilder::new()
        .with_url(url)
        .with_http_connector(connector)
        .build()
}

#[cfg(feature = "http")]
pub fn batteries_included_http_store(
    url: &str,
) -> object_store::Result<object_store::http::HttpStore> {
    object_store::http::HttpBuilder::new().with_url(url).build()
}
