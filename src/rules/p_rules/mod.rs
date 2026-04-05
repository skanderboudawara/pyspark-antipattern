//! Pandas interoperability rules (P001).
//! These rules flag `toPandas()` calls that are missing Arrow-based
//! optimisation, which can significantly reduce serialisation overhead.
pub mod p001;
