// PERF007: DataFrame used 2+ times in join / union operations without caching.
//
// When the same DataFrame appears as an input in two or more join or union
// statements without an intervening `.cache()` or `.persist(...)` call, Spark
// will re-execute its entire upstream DAG from scratch for each consumer.
//
// Example:
//   df = df.filter(col('country') == 'US')
//   df2 = df.join(cities, 'city_id')    # full DAG replayed
//   df3 = df.union(fallback)            # full DAG replayed again
//
// Fix:
//   df = df.filter(col('country') == 'US')
//   df = df.cache()
//   df2 = df.join(cities, 'city_id')    # reads from cache
//   df3 = df.union(fallback)            # reads from cache
//
// Trigger methods: join, crossJoin, union, unionAll, unionByName.
// Other DataFrame methods (show, collect, count, …) are intentionally ignored
// to avoid noise — only the branching / fan-out operations are relevant.
//
// Detection (per-scope, two-phase):
//   Phase 1 — pre-scan: build the set of variable names that are DataFrames
//     (receivers of known DataFrame methods, bare-Name args to CACHE_HINT_OPS,
//     and targets of assignments whose RHS contains a DataFrame method call).
//   Phase 2 — linear scan: for each statement, collect variable references that
//     appear as the receiver or a bare-Name argument of a CACHE_HINT_OP call.
//     Each variable is counted AT MOST ONCE per statement (deduplication) so
//     that chained calls like df1.union(df2).join(df3, 'id') do not cause df1
//     to be counted twice within that single expression.
//     When a variable reaches 2 across different statements without a
//     `.cache()` / `.persist(…)` assignment in between, it is flagged.
//
// Scoping: nested function definitions are treated as independent scopes.

use std::collections::{HashMap, HashSet};

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    spark_ops::{DATAFRAME_METHODS, NON_DATAFRAME_ROOTS},
    violation::{RuleId, Severity, Violation},
};

const ID: &str = "PERF007";

/// Operations where using an uncached DataFrame as input causes DAG replay.
const CACHE_HINT_OPS: &[&str] = &[
    "join",
    "crossJoin",
    "union",
    "unionAll",
    "unionByName",
];

// ── Non-DataFrame receiver guard ─────────────────────────────────────────────

/// Walk the receiver chain to its root `Name` and return `true` when that root
/// belongs to a known non-DataFrame namespace (os, sys, pathlib, …).
///
/// This prevents `os.path.join(…)`, `",".join(…)`, and similar stdlib calls
/// that happen to share a method name with CACHE_HINT_OPS from being treated
/// as Spark operations.
fn receiver_is_non_df(expr: &Expr) -> bool {
    let mut cur = expr;
    loop {
        match cur {
            Expr::Name(n) => return NON_DATAFRAME_ROOTS.contains(&n.id.as_str()),
            Expr::Call(c) => {
                if let Expr::Attribute(a) = c.func.as_ref() {
                    cur = a.value.as_ref();
                } else {
                    return false;
                }
            }
            Expr::Attribute(a) => cur = a.value.as_ref(),
            Expr::Constant(_) => return true, // string literal receiver: `",".join(…)`
            _ => return false,
        }
    }
}

// ── Phase 1: identify DataFrame variables ────────────────────────────────────

/// True when `expr` contains at least one call to a known DataFrame method
/// anywhere in its chain.
fn has_dataframe_method(expr: &Expr) -> bool {
    match expr {
        Expr::Call(c) => {
            if let Expr::Attribute(a) = c.func.as_ref() {
                if DATAFRAME_METHODS.contains(&a.attr.as_str()) {
                    return true;
                }
                return has_dataframe_method(&a.value);
            }
            false
        }
        Expr::Attribute(a) => has_dataframe_method(&a.value),
        _ => false,
    }
}

/// Recursively collect names that appear as the immediate `Name` receiver of a
/// known DataFrame method call, plus bare `Name` arguments of CACHE_HINT_OP
/// calls (since those arguments are almost always DataFrames).
fn collect_df_names(expr: &Expr, out: &mut HashSet<String>) {
    if let Expr::Call(c) = expr {
        if let Expr::Attribute(a) = c.func.as_ref() {
            let method = a.attr.as_str();

            // Immediate Name receiver of any DataFrame method.
            if DATAFRAME_METHODS.contains(&method) {
                if let Expr::Name(n) = a.value.as_ref() {
                    out.insert(n.id.to_string());
                }
            }

            // Bare Name arguments of join / union operations — but only when
            // the receiver is not a stdlib / non-DataFrame object.
            if CACHE_HINT_OPS.contains(&method) && !receiver_is_non_df(&a.value) {
                for arg in &c.args {
                    if let Expr::Name(n) = arg {
                        out.insert(n.id.to_string());
                    }
                }
            }

            collect_df_names(&a.value, out);
            for arg in &c.args {
                collect_df_names(arg, out);
            }
            for kw in &c.keywords {
                collect_df_names(&kw.value, out);
            }
        }
    }
}

/// Build the set of variable names that are DataFrames in this scope.
fn identify_df_vars(stmts: &[Stmt]) -> HashSet<String> {
    let mut df_vars = HashSet::new();
    for stmt in stmts {
        if matches!(stmt, Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_)) {
            continue;
        }
        match stmt {
            Stmt::Assign(a) => {
                if has_dataframe_method(&a.value) {
                    if let Some(Expr::Name(n)) = a.targets.first() {
                        df_vars.insert(n.id.to_string());
                    }
                }
                collect_df_names(&a.value, &mut df_vars);
            }
            Stmt::Expr(e) => {
                collect_df_names(&e.value, &mut df_vars);
            }
            _ => {}
        }
    }
    df_vars
}

// ── Phase 2: count join/union references ─────────────────────────────────────

/// True when `expr` is a `.cache()` or `.persist(…)` call.
fn is_cache_or_persist(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr {
        if let Expr::Attribute(a) = c.func.as_ref() {
            return matches!(a.attr.as_str(), "cache" | "persist");
        }
    }
    false
}

/// Walk down a call / attribute chain and return `(name, offset)` for the
/// leftmost `Name` node — but only if that name is in `df_vars`.
fn root_df_ref(expr: &Expr, df_vars: &HashSet<String>) -> Option<(String, u32)> {
    let mut cur = expr;
    loop {
        match cur {
            Expr::Name(n) => {
                return if df_vars.contains(n.id.as_str()) {
                    Some((n.id.to_string(), n.range.start().into()))
                } else {
                    None
                };
            }
            Expr::Call(c) => {
                if let Expr::Attribute(a) = c.func.as_ref() {
                    cur = a.value.as_ref();
                } else {
                    return None;
                }
            }
            Expr::Attribute(a) => cur = a.value.as_ref(),
            _ => return None,
        }
    }
}

/// Collect `(variable_name, byte_offset)` for each DataFrame variable that
/// appears as the **receiver or a bare-Name argument** of a CACHE_HINT_OP call
/// anywhere inside `expr`.
///
/// Results are later deduplicated per statement so that chained expressions
/// such as `df.union(df2).join(df3, 'id')` do not count `df` twice.
fn collect_refs(expr: &Expr, df_vars: &HashSet<String>, out: &mut Vec<(String, u32)>) {
    if let Expr::Call(c) = expr {
        if let Expr::Attribute(a) = c.func.as_ref() {
            if CACHE_HINT_OPS.contains(&a.attr.as_str()) && !receiver_is_non_df(&a.value) {
                // Collect the root of the receiver chain.
                if let Some(entry) = root_df_ref(a.value.as_ref(), df_vars) {
                    out.push(entry);
                }
                // Collect bare Name arguments (the other DataFrame in a join/union).
                for arg in &c.args {
                    if let Some(entry) = root_df_ref(arg, df_vars) {
                        out.push(entry);
                    }
                }
            }
            // Always recurse — there may be nested join/union calls.
            collect_refs(&a.value, df_vars, out);
            for arg in &c.args {
                collect_refs(arg, df_vars, out);
            }
            for kw in &c.keywords {
                collect_refs(&kw.value, df_vars, out);
            }
        }
    }
}

// ── Per-variable tracking state ───────────────────────────────────────────────

#[derive(Default)]
struct VarState {
    use_count: usize,
    is_cached: bool,
    flagged: bool,
}

// ── Core scope analysis ───────────────────────────────────────────────────────

fn check_scope(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    severity: Severity,
    index: &LineIndex,
) -> Vec<Violation> {
    let df_vars = identify_df_vars(stmts);
    let mut states: HashMap<String, VarState> = HashMap::new();
    let mut violations: Vec<Violation> = vec![];

    for stmt in stmts {
        if matches!(stmt, Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_)) {
            continue;
        }

        match stmt {
            Stmt::Assign(a) => {
                let rhs_is_cache = is_cache_or_persist(&a.value);

                let target: Option<String> = a.targets.first().and_then(|t| {
                    if let Expr::Name(n) = t {
                        Some(n.id.to_string())
                    } else {
                        None
                    }
                });

                // Collect join/union references from the RHS.
                let mut raw: Vec<(String, u32)> = vec![];
                collect_refs(&a.value, &df_vars, &mut raw);

                // Deduplicate within this statement: chained expressions like
                // `df.union(df2).join(df3)` must count `df` only once.
                let mut seen_names: HashSet<String> = HashSet::new();
                let refs: Vec<(String, u32)> = raw
                    .into_iter()
                    .filter(|(name, _)| seen_names.insert(name.clone()))
                    .collect();

                // Exclude the assignment target from its own ref counting
                // (pipeline steps: `df = df.join(…)` / `df = df.cache()`).
                let exclude = target.as_deref();

                for (name, offset) in &refs {
                    if Some(name.as_str()) == exclude {
                        continue;
                    }
                    let state = states.entry(name.clone()).or_default();
                    if state.is_cached || state.flagged {
                        continue;
                    }
                    state.use_count += 1;
                    if state.use_count >= 2 {
                        state.flagged = true;
                        let (line, col) = index.line_col(*offset);
                        let source_line = index.line_text(source, line).to_string();
                        violations.push(Violation {
                            rule_id: RuleId(ID.to_string()),
                            severity,
                            impact: crate::violation::Impact::Low,
                            file: file.to_string(),
                            line,
                            col,
                            source_line,
                            span_len: name.len(),
                        });
                    }
                }

                if let Some(name) = &target {
                    if rhs_is_cache {
                        let state = states.entry(name.clone()).or_default();
                        state.is_cached = true;
                        state.flagged = false;
                    } else {
                        states.insert(name.clone(), VarState::default());
                    }
                }
            }

            Stmt::Expr(e) => {
                let mut raw: Vec<(String, u32)> = vec![];
                collect_refs(&e.value, &df_vars, &mut raw);

                let mut seen_names: HashSet<String> = HashSet::new();
                let refs: Vec<(String, u32)> = raw
                    .into_iter()
                    .filter(|(name, _)| seen_names.insert(name.clone()))
                    .collect();

                for (name, offset) in &refs {
                    let state = states.entry(name.clone()).or_default();
                    if state.is_cached || state.flagged {
                        continue;
                    }
                    state.use_count += 1;
                    if state.use_count >= 2 {
                        state.flagged = true;
                        let (line, col) = index.line_col(*offset);
                        let source_line = index.line_text(source, line).to_string();
                        violations.push(Violation {
                            rule_id: RuleId(ID.to_string()),
                            severity,
                            impact: crate::violation::Impact::Low,
                            file: file.to_string(),
                            line,
                            col,
                            source_line,
                            span_len: name.len(),
                        });
                    }
                }
            }

            _ => {}
        }
    }

    violations.sort_by_key(|v| (v.line, v.col));
    violations
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let severity = config.severity_of(ID);
    let mut violations = vec![];

    violations.extend(check_scope(stmts, source, file, severity, index));

    for stmt in stmts {
        let body = match stmt {
            Stmt::FunctionDef(f) => &f.body,
            Stmt::AsyncFunctionDef(f) => &f.body,
            _ => continue,
        };
        violations.extend(check_scope(body, source, file, severity, index));
    }

    violations
}
