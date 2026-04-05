//! Core diagnostic types: rule identifiers, severity levels, impact ratings,
//! PySpark version constraints, and the `Violation` record produced by each rule.

/// Newtype wrapping a rule identifier string (e.g. `"D001"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuleId(pub String);

impl std::fmt::Display for RuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Diagnostic severity of a rule violation — controls exit code and prefix label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Causes a non-zero exit code; shown in red.
    Error,
    /// Informational; shown in yellow and does not affect exit code.
    Warning,
}

/// Performance impact of a rule violation.
/// Used to filter violations by minimum severity via `--severity` or `severity` in
/// `[tool.pyspark-antipattern]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Impact {
    Low,
    Medium,
    High,
}

impl std::fmt::Display for Impact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Impact::Low => write!(f, "LOW"),
            Impact::Medium => write!(f, "MEDIUM"),
            Impact::High => write!(f, "HIGH"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Impact {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(d)?;
        match s.to_lowercase().as_str() {
            "low" => Ok(Impact::Low),
            "medium" => Ok(Impact::Medium),
            "high" => Ok(Impact::High),
            other => Err(serde::de::Error::unknown_variant(other, &["low", "medium", "high"])),
        }
    }
}

/// Minimum PySpark version a rule applies to.
/// If the user sets `pyspark_version` in config, rules with a higher `since` version
/// are silenced — they reference APIs not yet available on that cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PySparkVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl PySparkVersion {
    /// Construct a `PySparkVersion` from its major, minor, and patch components.
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }
}

impl std::fmt::Display for PySparkVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.patch == 0 {
            write!(f, "{}.{}", self.major, self.minor)
        } else {
            write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
        }
    }
}

impl std::str::FromStr for PySparkVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        let parse = |p: &str| p.parse::<u32>().map_err(|_| format!("invalid version component: {p}"));
        match parts.as_slice() {
            [major, minor] => Ok(Self::new(parse(major)?, parse(minor)?, 0)),
            [major, minor, patch] => Ok(Self::new(parse(major)?, parse(minor)?, parse(patch)?)),
            _ => Err(format!("invalid pyspark_version '{s}'; expected 'X.Y' or 'X.Y.Z'")),
        }
    }
}

impl<'de> serde::Deserialize<'de> for PySparkVersion {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(d)?;
        s.parse::<Self>().map_err(serde::de::Error::custom)
    }
}

/// A single rule violation found in a source file.
#[derive(Debug, Clone)]
pub struct Violation {
    pub rule_id: RuleId,
    pub severity: Severity,
    /// Performance impact of this rule — set centrally in `checker.rs` via
    /// `reporter::rule_impact()` after violations are collected from rules.
    /// Rule implementations should use `..Default::default()` and not set this.
    pub impact: Impact,
    pub file: String,
    pub line: usize, // 1-based
    pub col: usize,  // 1-based
    pub source_line: String,
    pub span_len: usize,
}
