use std::collections::HashMap;

use rayon::prelude::*;
use rustpython_parser::{ast::{Mod, Stmt}, parse, Mode};
use walkdir::WalkDir;

use crate::{
    config::Config,
    line_index::LineIndex,
    noqa,
    rules::{perf_rules::perf003, s_rules::s004, ALL_RULES},
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

// ── Global function-cost pre-pass ────────────────────────────────────────────

/// Parse a file and return its top-level statements, silently skipping on error.
fn parse_stmts(path: &str) -> Option<Vec<Stmt>> {
    let source = std::fs::read_to_string(path).ok()?;
    let parsed = parse(&source, Mode::Module, path).ok()?;
    match parsed {
        Mod::Module(m) => Some(m.body),
        _ => None,
    }
}

/// Collect `from X import Y as Z` aliases from a file's statements.
/// Returns a map of `local_alias → original_name`.
fn collect_import_aliases(stmts: &[Stmt]) -> HashMap<String, String> {
    let mut aliases = HashMap::new();
    for stmt in stmts {
        if let rustpython_parser::ast::Stmt::ImportFrom(imp) = stmt {
            for alias in &imp.names {
                if let Some(asname) = &alias.asname {
                    aliases.insert(asname.to_string(), alias.name.to_string());
                }
            }
        }
    }
    aliases
}

/// Build a project-wide `function_name → weighted_distinct_cost` map from all
/// `paths`.  Phase 1 collects raw costs; phase 2 resolves import aliases.
fn build_global_fn_distinct_costs(paths: &[String]) -> HashMap<String, i64> {
    let empty: HashMap<String, i64> = HashMap::new();
    let mut global: HashMap<String, i64> = HashMap::new();

    for path in paths {
        if let Some(stmts) = parse_stmts(path) {
            global.extend(s004::build_fn_distinct_costs(&stmts, &empty));
        }
    }

    // Import alias resolution.
    let mut alias_entries: Vec<(String, i64)> = vec![];
    for path in paths {
        if let Some(stmts) = parse_stmts(path) {
            for (alias, original) in collect_import_aliases(&stmts) {
                if let Some(&cost) = global.get(&original) {
                    alias_entries.push((alias, cost));
                }
            }
        }
    }
    global.extend(alias_entries);
    global
}

/// Build a project-wide `function_name → shuffle_export_cost` map from all
/// `paths`.
///
/// **Phase 1** — collect each file's local function costs (direct shuffles
/// only, intra-file transitive calls resolved via the existing convergence
/// loop inside `build_fn_costs`).
///
/// **Phase 2** — resolve import aliases: if file B has
/// `from lib import helper as h` and `helper` has a known cost, add
/// `h → cost` so that calls to `h(df)` in B are priced correctly.
fn build_global_fn_costs(paths: &[String]) -> HashMap<String, usize> {
    // Phase 1: merge all per-file costs into one map (last-writer wins on
    // collisions — good enough for a linter that doesn't track modules).
    let mut global: HashMap<String, usize> = HashMap::new();
    for path in paths {
        if let Some(stmts) = parse_stmts(path) {
            global.extend(perf003::build_fn_costs(&stmts));
        }
    }

    // Phase 2: alias expansion — only needs one pass because aliases point
    // directly to function names already in `global`.
    let mut alias_entries: Vec<(String, usize)> = vec![];
    for path in paths {
        if let Some(stmts) = parse_stmts(path) {
            for (alias, original) in collect_import_aliases(&stmts) {
                if let Some(&cost) = global.get(&original) {
                    alias_entries.push((alias, cost));
                }
            }
        }
    }
    global.extend(alias_entries);

    global
}

// ── Path collection ───────────────────────────────────────────────────────────

fn collect_paths(root: &str, config: &Config) -> Vec<String> {
    if std::path::Path::new(root).is_file() {
        return vec![root.to_string()];
    }
    WalkDir::new(root)
        .into_iter()
        .filter_entry(|e| {
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
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Lint a file or recursively scan a directory for .py files.
pub fn check_path(root: &str, config: &Config) -> (Vec<Violation>, usize) {
    let paths = collect_paths(root, config);
    let file_count = paths.len();

    // Pre-pass: build a project-wide function cost map so PERF003 can price
    // calls to helpers defined in other files.
    //
    // When a single file is given we still scan its parent directory so that
    // cross-file helpers (e.g. `from lib import helper`) have a known cost.
    let scan_paths = if std::path::Path::new(root).is_file() {
        let parent = std::path::Path::new(root)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| ".".to_string());
        collect_paths(&parent, config)
    } else {
        paths.clone()
    };

    let mut config_with_global = config.clone();
    config_with_global.global_fn_costs = build_global_fn_costs(&scan_paths);
    config_with_global.global_fn_distinct_costs = build_global_fn_distinct_costs(&scan_paths);

    let config_ref = &config_with_global;

    // Main pass: lint all files in parallel.
    let all_violations: Vec<Violation> = paths
        .par_iter()
        .flat_map(|path| match check_file(path, config_ref) {
            Ok(v) => v,
            Err(msg) => {
                eprintln!("warning: {msg}");
                vec![]
            }
        })
        .collect();

    (all_violations, file_count)
}
