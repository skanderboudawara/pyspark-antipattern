#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuleId(pub String);

impl std::fmt::Display for RuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// Performance impact of a rule violation.
/// Used to filter violations by minimum severity via `--min-severity` or `min_severity` in
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
            Impact::Low    => write!(f, "LOW"),
            Impact::Medium => write!(f, "MEDIUM"),
            Impact::High   => write!(f, "HIGH"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Impact {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(d)?;
        match s.to_lowercase().as_str() {
            "low"    => Ok(Impact::Low),
            "medium" => Ok(Impact::Medium),
            "high"   => Ok(Impact::High),
            other    => Err(serde::de::Error::unknown_variant(
                other,
                &["low", "medium", "high"],
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Violation {
    pub rule_id:     RuleId,
    pub severity:    Severity,
    /// Performance impact of this rule — set centrally in `checker.rs` via
    /// `reporter::rule_impact()` after violations are collected from rules.
    pub impact:      Impact,
    pub file:        String,
    pub line:        usize, // 1-based
    pub col:         usize, // 1-based
    pub source_line: String,
    pub span_len:    usize,
}
