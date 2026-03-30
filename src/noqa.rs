use std::collections::{HashMap, HashSet};

use crate::violation::Violation;

/// Parse `# noqa: pap: RULE1, RULE2` comments from source.
/// Returns a map of 1-based line number → set of suppressed rule IDs.
/// The special ID `*` means "suppress all pap rules on this line".
pub fn parse_suppressions(source: &str) -> HashMap<usize, HashSet<String>> {
    let mut map: HashMap<usize, HashSet<String>> = HashMap::new();
    for (idx, line) in source.lines().enumerate() {
        let line_no = idx + 1;
        if let Some(pos) = line.find("# noqa: pap:") {
            let after = &line[pos + "# noqa: pap:".len()..];
            let ids: HashSet<String> = after
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty() && s.starts_with(|c: char| c.is_alphabetic()))
                .collect();
            map.insert(line_no, ids);
        } else if line.contains("# noqa: pap") {
            // Bare `# noqa: pap` — suppress every pap rule on this line.
            map.insert(line_no, HashSet::from(["*".to_string()]));
        }
    }
    map
}

/// Remove violations suppressed by noqa comments.
pub fn filter_suppressed(
    violations: Vec<Violation>,
    suppressions: &HashMap<usize, HashSet<String>>,
) -> Vec<Violation> {
    violations
        .into_iter()
        .filter(|v| match suppressions.get(&v.line) {
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
