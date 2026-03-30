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

#[derive(Debug, Clone)]
pub struct Violation {
    pub rule_id:     RuleId,
    pub severity:    Severity,
    pub file:        String,
    pub line:        usize, // 1-based
    pub col:         usize, // 1-based
    pub source_line: String,
    pub span_len:    usize,
}
