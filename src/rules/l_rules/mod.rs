//! Loop anti-pattern rules (L001–L003).
//! These rules detect DataFrame operations performed inside loops, which can
//! cause runaway lineage growth or repeated shuffles without checkpointing.
pub mod l001;
pub mod l002;
pub mod l003;
