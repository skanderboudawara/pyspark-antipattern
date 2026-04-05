//! General performance rules (PERF001–PERF008).
//! These rules detect inefficient Spark usage patterns such as excessive
//! shuffle operations, missing checkpoints, and improper caching strategies.
pub mod perf001;
pub mod perf002;
pub mod perf003;
pub mod perf004;
pub mod perf005;
pub mod perf006;
pub mod perf007;
pub mod perf008;
