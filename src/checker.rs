use rayon::prelude::*;
use rustpython_parser::{ast::Mod, parse, Mode};
use walkdir::WalkDir;

use crate::{
    config::Config,
    line_index::LineIndex,
    noqa,
    rules::ALL_RULES,
    violation::Violation,
};

/// Lint a single .py file and return violations (noqa-filtered and sorted).
pub fn check_file(path: &str, config: &Config) -> Result<Vec<Violation>, String> {
    let source = std::fs::read_to_string(path)
        .map_err(|e| format!("Cannot read {path}: {e}"))?;

    let parsed = parse(&source, Mode::Module, path)
        .map_err(|e| format!("Parse error in {path}: {e}"))?;

    let stmts = match parsed {
        Mod::Module(m) => m.body,
        _ => vec![],
    };

    let index = LineIndex::new(&source);
    let suppressions = noqa::parse_suppressions(&source);

    let mut violations: Vec<Violation> = ALL_RULES
        .iter()
        .flat_map(|rule_fn| rule_fn(&stmts, &source, path, config, &index))
        .filter(|v| !config.is_ignored(&v.rule_id.0))
        .collect();

    violations = noqa::filter_suppressed(violations, &suppressions);
    violations.sort_by_key(|v| (v.line, v.col, v.rule_id.0.clone()));
    Ok(violations)
}

/// Lint a file or recursively scan a directory for .py files.
pub fn check_path(root: &str, config: &Config) -> (Vec<Violation>, usize) {
    let paths: Vec<String> = if std::path::Path::new(root).is_file() {
        vec![root.to_string()]
    } else {
        WalkDir::new(root)
            .into_iter()
            .filter_entry(|e| {
                // Always allow the root entry itself; skip excluded directories.
                if e.depth() == 0 { return true; }
                if e.file_type().is_dir() {
                    let dir_name = e.file_name().to_string_lossy();
                    return !config.is_excluded_dir(&dir_name);
                }
                true
            })
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "py"))
            .map(|e| e.path().to_string_lossy().into_owned())
            .collect()
    };

    let file_count = paths.len();

    // Process files in parallel.
    let all_violations: Vec<Violation> = paths
        .par_iter()
        .flat_map(|path| match check_file(path, config) {
            Ok(v) => v,
            Err(msg) => {
                eprintln!("warning: {msg}");
                vec![]
            }
        })
        .collect();

    (all_violations, file_count)
}
