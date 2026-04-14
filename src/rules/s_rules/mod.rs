//! Shuffle-related anti-pattern rules (S001–S015).
//! These rules flag join, repartition, distinct, and explode patterns that
//! trigger unnecessary or costly network data exchanges.
pub mod s001;
pub mod s002;
pub mod s003;
pub mod s004;
pub mod s005;
pub mod s006;
pub mod s007;
pub mod s008;
pub mod s009;
pub mod s010;
pub mod s011;
pub mod s012;
pub mod s013;
pub mod s014;
pub mod s015;
