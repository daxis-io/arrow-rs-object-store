#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

manifest="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/Cargo.toml"
target="wasm32-unknown-unknown"

cargo check --locked --manifest-path "${manifest}" --target "${target}" \
  --no-default-features --features http-base
cargo check --locked --manifest-path "${manifest}" --target "${target}" \
  --no-default-features --features http-base,reqwest,web
cargo check --locked --manifest-path "${manifest}" --target "${target}" \
  --no-default-features --features http

graph="$(cargo tree --locked --manifest-path "${manifest}" --target "${target}" \
  --no-default-features --features http --edges normal --prefix none)"

denied='^(aws-lc-sys|hyper|hyper-util|liblzma-sys|native-tls|openssl-sys|ring|tempfile|tokio|walkdir|zstd-sys) v'
if printf '%s\n' "${graph}" | grep -E "${denied}"; then
  echo "denied native dependency found in the browser graph" >&2
  exit 1
fi

features="$(cargo tree --locked --manifest-path "${manifest}" --target "${target}" \
  --no-default-features --features http --edges features --prefix none)"
if printf '%s\n' "${features}" | grep -E '^object_store feature "(fs|aws|azure|gcp)"'; then
  echo "filesystem or cloud-provider batteries found in the browser graph" >&2
  exit 1
fi

echo "browser dependency graph is target-safe"
