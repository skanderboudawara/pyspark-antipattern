use std::collections::HashMap;
use std::sync::OnceLock;

static RULE_MARKDOWN: &[(&str, &str)] = &[
    ("D001", include_str!("../rules/driver/D001.md")),
    ("D002", include_str!("../rules/driver/D002.md")),
    ("D003", include_str!("../rules/driver/D003.md")),
    ("D004", include_str!("../rules/driver/D004.md")),
    ("D005", include_str!("../rules/driver/D005.md")),
    ("D006", include_str!("../rules/driver/D006.md")),
    ("D007", include_str!("../rules/driver/D007.md")),
    ("D008", include_str!("../rules/driver/D008.md")),
    ("F001", include_str!("../rules/format/F001.md")),
    ("F002", include_str!("../rules/format/F002.md")),
    ("F003", include_str!("../rules/format/F003.md")),
    ("F004", include_str!("../rules/format/F004.md")),
    ("F005", include_str!("../rules/format/F005.md")),
    ("F006", include_str!("../rules/format/F006.md")),
    ("F007", include_str!("../rules/format/F007.md")),
    ("F008", include_str!("../rules/format/F008.md")),
    ("F009", include_str!("../rules/format/F009.md")),
    ("F010", include_str!("../rules/format/F010.md")),
    ("F011", include_str!("../rules/format/F011.md")),
    ("F012", include_str!("../rules/format/F012.md")),
    ("L001", include_str!("../rules/looping/L001.md")),
    ("L002", include_str!("../rules/looping/L002.md")),
    ("L003", include_str!("../rules/looping/L003.md")),
    ("P001", include_str!("../rules/pandas/P001.md")),
    ("S001", include_str!("../rules/shuffle/S001.md")),
    ("S002", include_str!("../rules/shuffle/S002.md")),
    ("S003", include_str!("../rules/shuffle/S003.md")),
    ("S004", include_str!("../rules/shuffle/S004.md")),
    ("S005", include_str!("../rules/shuffle/S005.md")),
    ("S006", include_str!("../rules/shuffle/S006.md")),
    ("S007", include_str!("../rules/shuffle/S007.md")),
    ("S008", include_str!("../rules/shuffle/S008.md")),
    ("S009", include_str!("../rules/shuffle/S009.md")),
    ("S010", include_str!("../rules/shuffle/S010.md")),
    ("S011", include_str!("../rules/shuffle/S011.md")),
    ("S012", include_str!("../rules/shuffle/S012.md")),
    ("U001", include_str!("../rules/udf/U001.md")),
    ("U002", include_str!("../rules/udf/U002.md")),
    ("U003", include_str!("../rules/udf/U003.md")),
];

pub struct RuleContent {
    pub information: String,
    pub best_practice: String,
}

/// Parsed content cache — built once on first access, reused on every call.
static CACHE: OnceLock<HashMap<&'static str, RuleContent>> = OnceLock::new();

fn cache() -> &'static HashMap<&'static str, RuleContent> {
    CACHE.get_or_init(|| {
        RULE_MARKDOWN
            .iter()
            .map(|(id, md)| (*id, parse_markdown(md)))
            .collect()
    })
}

pub fn get_content(rule_id: &str) -> Option<&'static RuleContent> {
    cache().get(rule_id)
}

fn parse_markdown(md: &str) -> RuleContent {
    let mut info_lines: Vec<&str> = vec![];
    let mut bp_lines: Vec<&str> = vec![];
    let mut current: Option<&str> = None;

    for line in md.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("## ") {
            let header = trimmed[3..].to_lowercase();
            if header.starts_with("information") || header.starts_with("why") {
                current = Some("info");
            } else if header.starts_with("best practice") {
                current = Some("bp");
            } else {
                current = None;
            }
            continue;
        }
        if trimmed.starts_with("# ") {
            current = None;
            continue;
        }
        match current {
            Some("info") => info_lines.push(line),
            Some("bp") => bp_lines.push(line),
            _ => {}
        }
    }

    RuleContent {
        information: info_lines.join("\n").trim().to_string(),
        best_practice: bp_lines.join("\n").trim().to_string(),
    }
}

/// Strip basic markdown formatting for plain-text terminal output.
pub fn strip_md(s: &str) -> String {
    s.replace('`', "")
        .replace("**", "")
        .replace('*', "")
        .replace("  ", " ")
}
