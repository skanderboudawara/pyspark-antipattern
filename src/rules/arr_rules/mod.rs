//! Array-related lint rules (ARR001–ARR006).
//! These rules flag inefficient patterns involving array aggregations and
//! window functions that can be replaced with more efficient Spark equivalents.
pub mod arr001;
pub mod arr002;
pub mod arr003;
pub mod arr004;
pub mod arr005;
pub mod arr006;
