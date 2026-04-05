//! Driver-side anti-pattern rules (D001–D008).
//! These rules flag operations that pull data to the driver or expose
//! production-unsafe debugging calls such as `.collect()`, `.show()`, or `.rdd`.
pub mod d001;
pub mod d002;
pub mod d003;
pub mod d004;
pub mod d005;
pub mod d006;
pub mod d007;
pub mod d008;
