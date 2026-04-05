//! S004: Too many `.distinct()` operations in one file beyond the configured threshold.
//! Loop-aware (range(N) = N, while = 99) and cross-file function-cost-aware.
use std::collections::HashMap;

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{for_loop_iters, method_violation},
    violation::{RuleId, Severity, Violation},
    visitor::{Visitor, walk_expr},
};

const ID: &str = "S004";
const WHILE_ASSUMED_ITERS: i64 = 99;

// ── Two-level cost lookup ─────────────────────────────────────────────────────

/// Look up the cost of `key` in `local` first, then `global`.
/// Avoids cloning the global map into a per-file merged map.
#[inline]
fn lookup(local: &HashMap<String, i64>, global: &HashMap<String, i64>, key: &str) -> i64 {
    local.get(key).or_else(|| global.get(key)).copied().unwrap_or(0)
}

// ── Expression-level counter ──────────────────────────────────────────────────

/// Counts `.distinct()` calls and bare function-call costs in one expression.
struct ExprCounter<'a> {
    count: i64,
    local_costs: &'a HashMap<String, i64>,
    global_costs: &'a HashMap<String, i64>,
}

impl<'a> Visitor for ExprCounter<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            match call.func.as_ref() {
                Expr::Attribute(attr) if attr.attr.as_str() == "distinct" => {
                    self.count += 1;
                }
                Expr::Name(n) => {
                    self.count += lookup(self.local_costs, self.global_costs, n.id.as_str());
                }
                _ => {}
            }
        }
        walk_expr(self, expr);
    }
}

// ── Occurrence collector ──────────────────────────────────────────────────────

/// Collects every `.distinct()` call site **and** every bare function-call site
/// whose function contributes distinct() operations (for cross-file helpers).
struct OccurrenceCollector<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: Severity,
    local_costs: &'a HashMap<String, i64>,
    global_costs: &'a HashMap<String, i64>,
    occurrences: Vec<Violation>,
}

impl<'a> Visitor for OccurrenceCollector<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            match call.func.as_ref() {
                Expr::Attribute(attr) if attr.attr.as_str() == "distinct" => {
                    self.occurrences.push(method_violation(
                        attr,
                        "distinct",
                        self.source,
                        self.file,
                        self.index,
                        self.severity,
                        ID,
                    ));
                }
                Expr::Name(n) if lookup(self.local_costs, self.global_costs, n.id.as_str()) > 0 => {
                    // This function call brings in distinct() from another scope.
                    let start: u32 = n.range.start().into();
                    let (line, col) = self.index.line_col(start, self.source);
                    let source_line = self.index.line_text(self.source, line).to_string();
                    self.occurrences.push(Violation {
                        rule_id: RuleId(ID.to_string()),
                        severity: self.severity,
                        impact: crate::violation::Impact::Low,
                        file: self.file.to_string(),
                        line,
                        col,
                        source_line,
                        span_len: n.id.len() + 2, // name + "()"
                    });
                }
                _ => {}
            }
        }
        walk_expr(self, expr);
    }
}

// ── Weighted statement scanner ────────────────────────────────────────────────

/// Walk `stmts` and return the total weighted distinct() count.
///
/// Function definitions are **not** recursed into — their cost is captured in
/// `local_costs`/`global_costs` so that only call sites (not definitions) are
/// counted.  This avoids double-counting when a helper is defined and called in
/// the same file.
fn weighted_count(stmts: &[Stmt], multiplier: i64, local: &HashMap<String, i64>, global: &HashMap<String, i64>) -> i64 {
    let mut total = 0i64;
    for stmt in stmts {
        match stmt {
            Stmt::For(f) => {
                let iters = for_loop_iters(&f.iter).unwrap_or(i64::MAX / multiplier.max(1));
                let m = multiplier.saturating_mul(iters);
                total = total.saturating_add(weighted_count(&f.body, m, local, global));
                total = total.saturating_add(weighted_count(&f.orelse, multiplier, local, global));
            }
            Stmt::While(w) => {
                let m = multiplier.saturating_mul(WHILE_ASSUMED_ITERS);
                total = total.saturating_add(weighted_count(&w.body, m, local, global));
                total = total.saturating_add(weighted_count(&w.orelse, multiplier, local, global));
            }
            Stmt::If(i) => {
                total = total.saturating_add(weighted_count(&i.body, multiplier, local, global));
                total = total.saturating_add(weighted_count(&i.orelse, multiplier, local, global));
            }
            Stmt::With(w) => {
                total = total.saturating_add(weighted_count(&w.body, multiplier, local, global));
            }
            Stmt::Try(t) => {
                total = total.saturating_add(weighted_count(&t.body, multiplier, local, global));
                total = total.saturating_add(weighted_count(&t.orelse, multiplier, local, global));
                total = total.saturating_add(weighted_count(&t.finalbody, multiplier, local, global));
            }
            // Function definitions are NOT recursed — costs come from local/global.
            Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_) => {}
            Stmt::Expr(e) => {
                let mut counter = ExprCounter {
                    count: 0,
                    local_costs: local,
                    global_costs: global,
                };
                counter.visit_expr(&e.value);
                total = total.saturating_add(counter.count.saturating_mul(multiplier));
            }
            Stmt::Assign(a) => {
                let mut counter = ExprCounter {
                    count: 0,
                    local_costs: local,
                    global_costs: global,
                };
                counter.visit_expr(&a.value);
                total = total.saturating_add(counter.count.saturating_mul(multiplier));
            }
            _ => {}
        }
    }
    total
}

// ── Per-function cost computation ─────────────────────────────────────────────

/// Compute the total weighted distinct() cost contributed by a function body.
fn body_distinct_cost(body: &[Stmt], fn_costs: &HashMap<String, i64>) -> i64 {
    // During convergence we use a single merged map; the clone is bounded to
    // the number of locally-defined functions, not the global map.
    weighted_count(body, 1, fn_costs, &HashMap::new())
}

/// Build a `function_name → weighted_distinct_cost` map for all top-level
/// function definitions in `stmts`.  Uses iterative convergence (up to 10
/// rounds) to handle transitive calls.
///
/// `pub(crate)` so `checker.rs` can call this during the global pre-pass.
pub(crate) fn build_fn_distinct_costs(stmts: &[Stmt], seed: &HashMap<String, i64>) -> HashMap<String, i64> {
    let mut fn_bodies: Vec<(String, &[Stmt])> = vec![];
    for stmt in stmts {
        match stmt {
            Stmt::FunctionDef(f) => fn_bodies.push((f.name.to_string(), &f.body)),
            Stmt::AsyncFunctionDef(f) => fn_bodies.push((f.name.to_string(), &f.body)),
            _ => {}
        }
    }

    // Convergence uses a merged map — bounded to functions in this file + seed.
    let mut fn_costs = seed.clone();
    for _ in 0..10 {
        let mut changed = false;
        for (name, body) in &fn_bodies {
            let new_cost = body_distinct_cost(body, &fn_costs);
            let old_cost = fn_costs.get(name.as_str()).copied().unwrap_or(0);
            if new_cost != old_cost {
                fn_costs.insert(name.clone(), new_cost);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Return only the functions defined in this set of stmts.
    fn_bodies
        .iter()
        .filter_map(|(name, _)| fn_costs.get(name).map(|&c| (name.clone(), c)))
        .collect()
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Scan `stmts` for `.distinct()` usage exceeding the configured threshold and flag call sites.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    // Build only the file-local function costs; the global map is passed by
    // reference so we never clone it per file.
    let local_costs = build_fn_distinct_costs(stmts, &config.global_fn_distinct_costs);
    let global_costs = &config.global_fn_distinct_costs;

    let weighted = weighted_count(stmts, 1, &local_costs, global_costs);
    if weighted <= config.distinct_threshold as i64 {
        return vec![];
    }

    // Emit a violation for every actual call site (direct .distinct() or
    // cross-scope function calls that bring in distinct operations).
    let mut collector = OccurrenceCollector {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        local_costs: &local_costs,
        global_costs,
        occurrences: vec![],
    };
    for s in stmts {
        collector.visit_stmt(s);
    }
    collector.occurrences
}
