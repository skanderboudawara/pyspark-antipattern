use std::collections::{HashMap, HashSet};

use crate::violation::Violation;

/// Parsed suppression directives for a single source file.
pub struct Suppressions {
    /// When `true`, every violation in the file is suppressed.
    /// Triggered by `# noqa: pap: FILE` anywhere in the file.
    pub file_wide: bool,
    /// Per-line suppression: 1-based line → set of rule IDs to suppress.
    /// The special ID `*` means "all pap rules on this line".
    pub lines: HashMap<usize, HashSet<String>>,
}

/// Parse `# noqa: pap: RULE1, RULE2` comments from source.
///
/// Special forms:
/// - `# noqa: pap`            → suppress all pap rules on that line.
/// - `# noqa: pap: FILE`      → suppress every violation in the entire file.
pub fn parse_suppressions(source: &str) -> Suppressions {
    let mut lines: HashMap<usize, HashSet<String>> = HashMap::new();
    let mut file_wide = false;

    for (idx, line) in source.lines().enumerate() {
        let line_no = idx + 1;
        if let Some(pos) = line.find("# noqa: pap:") {
            let after = &line[pos + "# noqa: pap:".len()..];
            let ids: HashSet<String> = after
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty() && s.starts_with(|c: char| c.is_alphabetic()))
                .collect();
            // `# noqa: pap: FILE` → suppress the entire file
            if ids.contains("FILE") {
                file_wide = true;
            } else {
                lines.insert(line_no, ids);
            }
        } else if line.contains("# noqa: pap") {
            // Bare `# noqa: pap` — suppress every pap rule on this line.
            lines.insert(line_no, HashSet::from(["*".to_string()]));
        }
    }

    Suppressions { file_wide, lines }
}

/// Remove violations suppressed by noqa comments.
pub fn filter_suppressed(violations: Vec<Violation>, suppressions: &Suppressions) -> Vec<Violation> {
    if suppressions.file_wide {
        return vec![];
    }
    violations
        .into_iter()
        .filter(|v| match suppressions.lines.get(&v.line) {
            None => true,
            Some(ids) => {
                if ids.contains("*") {
                    return false;
                }
                !ids.contains(&v.rule_id.0)
            }
        })
        .collect()
}
