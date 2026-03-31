use std::collections::HashMap;

use rayon::prelude::*;
use rustpython_parser::{ast::Mod, parse, Mode};
use walkdir::WalkDir;

use crate::{
    config::Config,
    line_index::LineIndex,
    noqa,
    rules::{perf_rules::perf003, s_rules::{s004, s008}, ALL_RULES},
    violation::Violation,
};

/// All costs extracted from a single file in one parse.
struct FileCosts {
    fn_costs:         HashMap<String, usize>,
    fn_distinct_costs: HashMap<String, i64>,
    fn_explode_costs:  HashMap<String, i64>,
    /// `local_alias → original_name` from `from X import Y as Z`
    import_aliases:   HashMap<String, String>,
}

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

/// Parse one file and extract all cost information in a single pass.
/// Returns `None` if the file cannot be read or parsed.
fn extract_file_costs(path: &str) -> Option<FileCosts> {
    let source = std::fs::read_to_string(path).ok()?;
    let parsed = parse(&source, Mode::Module, path).ok()?;
    let stmts = match parsed {
        Mod::Module(m) => m.body,
        _ => return None,
    };

    let empty_i64: HashMap<String, i64> = HashMap::new();

    // Collect import aliases: `from X import Y as Z` → Z → Y
    let mut import_aliases = HashMap::new();
    for stmt in &stmts {
        if let rustpython_parser::ast::Stmt::ImportFrom(imp) = stmt {
            for alias in &imp.names {
                if let Some(asname) = &alias.asname {
                    import_aliases.insert(asname.to_string(), alias.name.to_string());
                }
            }
        }
    }

    Some(FileCosts {
        fn_costs:          perf003::build_fn_costs(&stmts),
        fn_distinct_costs: s004::build_fn_distinct_costs(&stmts, &empty_i64),
        fn_explode_costs:  s008::build_fn_explode_costs(&stmts, &empty_i64),
        import_aliases,
    })
}

/// Build all three project-wide function-cost maps from `paths` in parallel.
///
/// Each file is parsed **once** (in parallel via rayon).  After merging, a
/// single alias-resolution pass propagates costs through `from X import Y as Z`
/// imports.
fn build_all_global_costs(
    paths: &[String],
) -> (HashMap<String, usize>, HashMap<String, i64>, HashMap<String, i64>) {
    // Phase 1 — parse all files in parallel.
    let all_costs: Vec<FileCosts> = paths
        .par_iter()
        .filter_map(|p| extract_file_costs(p))
        .collect();

    // Phase 2 — sequential merge (last-writer wins on name collisions).
    let mut global_fn:       HashMap<String, usize> = HashMap::new();
    let mut global_distinct: HashMap<String, i64>   = HashMap::new();
    let mut global_explode:  HashMap<String, i64>   = HashMap::new();

    for fc in &all_costs {
        global_fn.extend(fc.fn_costs.iter().map(|(k, v)| (k.clone(), *v)));
        global_distinct.extend(fc.fn_distinct_costs.iter().map(|(k, v)| (k.clone(), *v)));
        global_explode.extend(fc.fn_explode_costs.iter().map(|(k, v)| (k.clone(), *v)));
    }

    // Phase 3 — alias resolution: `from lib import helper as h` → h gets
    // the same cost as helper, for every cost map.
    let mut fn_aliases:       Vec<(String, usize)> = vec![];
    let mut distinct_aliases: Vec<(String, i64)>   = vec![];
    let mut explode_aliases:  Vec<(String, i64)>   = vec![];

    for fc in &all_costs {
        for (alias, original) in &fc.import_aliases {
            if let Some(&c) = global_fn.get(original.as_str())       { fn_aliases.push((alias.clone(), c)); }
            if let Some(&c) = global_distinct.get(original.as_str()) { distinct_aliases.push((alias.clone(), c)); }
            if let Some(&c) = global_explode.get(original.as_str())  { explode_aliases.push((alias.clone(), c)); }
        }
    }

    global_fn.extend(fn_aliases);
    global_distinct.extend(distinct_aliases);
    global_explode.extend(explode_aliases);

    (global_fn, global_distinct, global_explode)
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
///
/// `on_file` is called once per file (from a rayon worker thread) as soon as
/// that file's violations are ready.  Use it to stream output to the terminal
/// rather than waiting for all files to finish.  The callback must be `Sync`
/// because rayon may call it from multiple threads concurrently.
pub fn check_path(
    root: &str,
    config: &Config,
    on_file: &(dyn Fn(Vec<Violation>) + Sync),
) -> usize {
    let paths = collect_paths(root, config);
    let file_count = paths.len();

    eprintln!("Scanning {} file(s) — building cross-file cost maps…", file_count);

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

    let (gfn, gdistinct, gexplode) = build_all_global_costs(&scan_paths);
    let mut config_with_global = config.clone();
    config_with_global.global_fn_costs          = gfn;
    config_with_global.global_fn_distinct_costs = gdistinct;
    config_with_global.global_fn_explode_costs  = gexplode;

    let config_ref = &config_with_global;

    eprintln!("Linting {} file(s)…", file_count);

    // Main pass: lint all files in parallel, streaming results via on_file.
    paths.par_iter().for_each(|path| {
        let violations = match check_file(path, config_ref) {
            Ok(v)    => v,
            Err(msg) => { eprintln!("warning: {msg}"); vec![] }
        };
        on_file(violations);
    });

    file_count
}
