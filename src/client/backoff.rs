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

#[cfg(any(
    test,
    all(
        feature = "rand",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    )
))]
use rand::{Rng, RngExt, rng};

/// Exponential backoff with decorrelated jitter algorithm
///
/// The first backoff will always be `init_backoff`.
///
/// Subsequent backoffs will pick a random value between `init_backoff` and
/// `base * previous` where `previous` is the duration of the previous backoff
///
/// See <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// The initial backoff duration
    pub init_backoff: Duration,
    /// The maximum backoff duration
    pub max_backoff: Duration,
    /// The multiplier to use for the next backoff duration
    pub base: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(15),
            base: 2.,
        }
    }
}

/// [`Backoff`] can be created from a [`BackoffConfig`]
///
/// Consecutive calls to [`Backoff::next`] will return the next backoff interval
///
pub(crate) struct Backoff {
    init_backoff: f64,
    next_backoff_secs: f64,
    max_backoff_secs: f64,
    base: f64,
    jitter: Jitter,
}

enum Jitter {
    #[allow(dead_code)]
    Deterministic,
    #[cfg(any(
        test,
        all(
            feature = "rand",
            not(all(target_arch = "wasm32", target_os = "unknown"))
        )
    ))]
    Random(Option<Box<dyn Rng + Sync + Send>>),
    #[cfg(all(feature = "web", target_arch = "wasm32", target_os = "unknown"))]
    Web,
}

impl std::fmt::Debug for Backoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backoff")
            .field("init_backoff", &self.init_backoff)
            .field("next_backoff_secs", &self.next_backoff_secs)
            .field("max_backoff_secs", &self.max_backoff_secs)
            .field("base", &self.base)
            .finish()
    }
}

impl Backoff {
    /// Create a new [`Backoff`] from the provided [`BackoffConfig`]
    #[allow(dead_code)]
    pub(crate) fn new(config: &BackoffConfig) -> Self {
        Self::new_with_jitter(config, Jitter::Deterministic)
    }

    #[cfg(all(
        feature = "rand",
        not(all(target_arch = "wasm32", target_os = "unknown"))
    ))]
    pub(crate) fn new_randomized(config: &BackoffConfig) -> Self {
        Self::new_with_rng(config, None)
    }

    #[cfg(all(feature = "web", target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn new_web(config: &BackoffConfig) -> Self {
        Self::new_with_jitter(config, Jitter::Web)
    }

    /// Creates a new `Backoff` with the optional `rng`
    ///
    /// Used [`rand::rng()`] if no rng provided
    #[cfg(any(
        test,
        all(
            feature = "rand",
            not(all(target_arch = "wasm32", target_os = "unknown"))
        )
    ))]
    pub(crate) fn new_with_rng(
        config: &BackoffConfig,
        rng: Option<Box<dyn Rng + Sync + Send>>,
    ) -> Self {
        Self::new_with_jitter(config, Jitter::Random(rng))
    }

    fn new_with_jitter(config: &BackoffConfig, jitter: Jitter) -> Self {
        let init_backoff = config.init_backoff.as_secs_f64();
        Self {
            init_backoff,
            next_backoff_secs: init_backoff,
            max_backoff_secs: config.max_backoff.as_secs_f64(),
            base: config.base,
            jitter,
        }
    }

    /// Returns the next backoff duration to wait for
    pub(crate) fn next(&mut self) -> Duration {
        let range = self.init_backoff..(self.next_backoff_secs * self.base);

        let next = match &mut self.jitter {
            Jitter::Deterministic => (range.start + range.end) / 2.,
            #[cfg(any(
                test,
                all(
                    feature = "rand",
                    not(all(target_arch = "wasm32", target_os = "unknown"))
                )
            ))]
            Jitter::Random(Some(rng)) => rng.random_range(range),
            #[cfg(any(
                test,
                all(
                    feature = "rand",
                    not(all(target_arch = "wasm32", target_os = "unknown"))
                )
            ))]
            Jitter::Random(None) => rng().random_range(range),
            #[cfg(all(feature = "web", target_arch = "wasm32", target_os = "unknown"))]
            Jitter::Web => range.start + (range.end - range.start) * js_sys::Math::random(),
        };

        let next_backoff = self.max_backoff_secs.min(next);
        Duration::from_secs_f64(std::mem::replace(&mut self.next_backoff_secs, next_backoff))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use rand::{TryRng, rand_core::utils::fill_bytes_via_next_word};

    use super::*;

    struct FixedRng(u64);

    impl TryRng for FixedRng {
        type Error = Infallible;

        fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
            Ok(self.0 as _)
        }

        fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
            Ok(self.0)
        }

        fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Self::Error> {
            fill_bytes_via_next_word(dst, || self.try_next_u64())
        }
    }

    #[test]
    fn test_backoff() {
        let init_backoff_secs = 1.;
        let max_backoff_secs = 500.;
        let base = 3.;

        let config = BackoffConfig {
            init_backoff: Duration::from_secs_f64(init_backoff_secs),
            max_backoff: Duration::from_secs_f64(max_backoff_secs),
            base,
        };

        let assert_fuzzy_eq = |a: f64, b: f64| assert!((b - a).abs() < 0.0001, "{a} != {b}");

        // Create a static rng that takes the minimum of the range
        let rng = Box::new(FixedRng(0));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        for _ in 0..20 {
            assert_eq!(backoff.next().as_secs_f64(), init_backoff_secs);
        }

        // Create a static rng that takes the maximum of the range
        let rng = Box::new(FixedRng(u64::MAX));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        for i in 0..20 {
            let value = (base.powi(i) * init_backoff_secs).min(max_backoff_secs);
            assert_fuzzy_eq(backoff.next().as_secs_f64(), value);
        }

        // Create a static rng that takes the mid point of the range
        let rng = Box::new(FixedRng(u64::MAX / 2));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        let mut value = init_backoff_secs;
        for _ in 0..20 {
            assert_fuzzy_eq(backoff.next().as_secs_f64(), value);
            value =
                (init_backoff_secs + (value * base - init_backoff_secs) / 2.).min(max_backoff_secs);
        }
    }

    #[test]
    fn test_host_neutral_backoff_is_deterministic() {
        let config = BackoffConfig {
            init_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
            base: 2.,
        };
        let mut left = Backoff::new(&config);
        let mut right = Backoff::new(&config);

        for _ in 0..16 {
            assert_eq!(left.next(), right.next());
        }
    }
}
