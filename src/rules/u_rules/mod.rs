//! UDF (User-Defined Function) anti-pattern rules (U001–U007).
//! These rules discourage unnecessary UDFs and flag patterns inside UDF bodies
//! that should use Spark built-in higher-order functions instead.
pub mod u001;
pub mod u002;
pub mod u003;
pub mod u004;
pub mod u005;
pub mod u006;
pub mod u007;
