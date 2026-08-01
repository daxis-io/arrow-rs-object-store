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

use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error(
    "delayed retry requires a native Tokio timer or the explicit browser `web` host capability"
)]
pub(crate) struct RetryRuntimeError;

pub(crate) struct RetryRuntime {
    inner: RuntimeInner,
}

enum RuntimeInner {
    #[cfg(all(
        feature = "tokio",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    ))]
    Native(std::time::Instant),
    #[cfg(all(feature = "web", target_arch = "wasm32", target_os = "unknown"))]
    Web(web_time::Instant),
    Unsupported,
}

impl RetryRuntime {
    pub(crate) fn new() -> Self {
        #[cfg(all(feature = "web", target_arch = "wasm32", target_os = "unknown"))]
        {
            return Self {
                inner: RuntimeInner::Web(web_time::Instant::now()),
            };
        }

        #[cfg(all(
            feature = "tokio",
            not(all(target_arch = "wasm32", target_os = "unknown"))
        ))]
        {
            return Self {
                inner: RuntimeInner::Native(std::time::Instant::now()),
            };
        }

        #[allow(unreachable_code)]
        Self {
            inner: RuntimeInner::Unsupported,
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        match &self.inner {
            #[cfg(all(
                feature = "tokio",
                not(all(target_arch = "wasm32", target_os = "unknown"))
            ))]
            RuntimeInner::Native(start) => start.elapsed(),
            #[cfg(all(feature = "web", target_arch = "wasm32", target_os = "unknown"))]
            RuntimeInner::Web(start) => start.elapsed(),
            RuntimeInner::Unsupported => Duration::ZERO,
        }
    }

    pub(crate) async fn sleep(&self, duration: Duration) -> Result<(), RetryRuntimeError> {
        let _ = duration;
        match &self.inner {
            #[cfg(all(
                feature = "tokio",
                not(all(target_arch = "wasm32", target_os = "unknown"))
            ))]
            RuntimeInner::Native(_) => {
                tokio::time::sleep(duration).await;
                Ok(())
            }
            #[cfg(all(feature = "web", target_arch = "wasm32", target_os = "unknown"))]
            RuntimeInner::Web(_) => {
                let (sender, receiver) = futures_channel::oneshot::channel();
                wasm_bindgen_futures::spawn_local(async move {
                    let mut millis = duration.as_millis().max(1);
                    while millis != 0 {
                        let next = millis.min(u32::MAX as u128) as u32;
                        gloo_timers::future::TimeoutFuture::new(next).await;
                        millis -= u128::from(next);
                    }
                    let _ = sender.send(());
                });
                receiver.await.map_err(|_| RetryRuntimeError)
            }
            RuntimeInner::Unsupported => Err(RetryRuntimeError),
        }
    }
}

#[cfg(all(
    test,
    not(any(
        all(
            feature = "tokio",
            not(all(target_arch = "wasm32", target_os = "unknown"))
        ),
        all(feature = "web", target_arch = "wasm32", target_os = "unknown")
    ))
))]
mod tests {
    use super::*;

    #[test]
    fn host_neutral_runtime_rejects_delayed_retry() {
        let runtime = RetryRuntime::new();
        let error = futures_executor::block_on(runtime.sleep(Duration::from_millis(1)))
            .expect_err("host-neutral delayed retry unexpectedly succeeded");
        assert!(error.to_string().contains("delayed retry requires"));
    }
}
