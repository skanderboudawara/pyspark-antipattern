//! Public API for the `pyspark-antipattern` linter library.
//! Re-exports all sub-modules so downstream crates can access checkers,
//! configuration, and violation types through this single crate root.
pub mod checker;
pub mod config;
pub mod line_index;
pub mod noqa;
pub mod reporter;
pub mod rule_content;
pub mod rules;
pub mod spark_ops;
pub mod violation;
pub mod visitor;
