// S008: Overusing explode() / explode_outer() (threshold configurable)
//
// Loop-aware: an explode() inside range(N) counts as N occurrences;
// inside a while loop it counts as 99 (assumed iterations).
//
// Function-call-aware: if a helper function contains M explode() calls,
// every call to that helper contributes M to the file's running total.
// Cross-file helpers are resolved via config.global_fn_explode_costs,
// which is populated by checker.rs before the violation scan.
use std::collections::HashMap;

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{for_loop_iters, method_violation},
    violation::{RuleId, Severity, Violation},
    visitor::{walk_expr, Visitor},
};

const ID: &str = "S008";
const WHILE_ASSUMED_ITERS: i64 = 99;

fn is_explode(name: &str) -> bool {
    matches!(name, "explode" | "explode_outer")
}

// ── Expression-level counter ──────────────────────────────────────────────────

struct ExprCounter<'a> {
    count: i64,
    fn_costs: &'a HashMap<String, i64>,
}

impl<'a> Visitor for ExprCounter<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            match call.func.as_ref() {
                Expr::Attribute(attr) if is_explode(attr.attr.as_str()) => self.count += 1,
                Expr::Name(n) if is_explode(n.id.as_str()) => self.count += 1,
                Expr::Name(n) => {
                    self.count += self.fn_costs.get(n.id.as_str()).copied().unwrap_or(0);
                }
                _ => {}
            }
        }
        walk_expr(self, expr);
    }
}

// ── Occurrence collector ──────────────────────────────────────────────────────

struct OccurrenceCollector<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: Severity,
    fn_costs: &'a HashMap<String, i64>,
    occurrences: Vec<Violation>,
}

impl<'a> Visitor for OccurrenceCollector<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            match call.func.as_ref() {
                Expr::Attribute(attr) if is_explode(attr.attr.as_str()) => {
                    self.occurrences.push(method_violation(
                        attr, attr.attr.as_str(), self.source, self.file, self.index,
                        self.severity, ID,
                    ));
                }
                Expr::Name(n) if is_explode(n.id.as_str()) => {
                    let offset: u32 = call.range.start().into();
                    let (line, col) = self.index.line_col(offset);
                    let source_line = self.index.line_text(self.source, line).to_string();
                    self.occurrences.push(Violation {
                        rule_id: RuleId(ID.to_string()),
                        severity: self.severity,
                        file: self.file.to_string(),
                        line, col,
                        source_line,
                        span_len: n.id.as_str().len() + 2,
                    });
                }
                // Bare function call that introduces explode() from another scope.
                Expr::Name(n)
                    if self.fn_costs.get(n.id.as_str()).copied().unwrap_or(0) > 0 =>
                {
                    let start: u32 = n.range.start().into();
                    let (line, col) = self.index.line_col(start);
                    let source_line = self.index.line_text(self.source, line).to_string();
                    self.occurrences.push(Violation {
                        rule_id: RuleId(ID.to_string()),
                        severity: self.severity,
                        file: self.file.to_string(),
                        line, col,
                        source_line,
                        span_len: n.id.len() + 2,
                    });
                }
                _ => {}
            }
        }
        walk_expr(self, expr);
    }
}

// ── Weighted statement scanner ────────────────────────────────────────────────

fn weighted_count(stmts: &[Stmt], multiplier: i64, fn_costs: &HashMap<String, i64>) -> i64 {
    let mut total = 0i64;
    for stmt in stmts {
        match stmt {
            Stmt::For(f) => {
                let iters = for_loop_iters(&f.iter).unwrap_or(i64::MAX / multiplier);
                let m = multiplier.saturating_mul(iters);
                total += weighted_count(&f.body, m, fn_costs);
                total += weighted_count(&f.orelse, multiplier, fn_costs);
            }
            Stmt::While(w) => {
                let m = multiplier.saturating_mul(WHILE_ASSUMED_ITERS);
                total += weighted_count(&w.body, m, fn_costs);
                total += weighted_count(&w.orelse, multiplier, fn_costs);
            }
            Stmt::If(i) => {
                total += weighted_count(&i.body, multiplier, fn_costs);
                total += weighted_count(&i.orelse, multiplier, fn_costs);
            }
            Stmt::With(w) => total += weighted_count(&w.body, multiplier, fn_costs),
            Stmt::Try(t) => {
                total += weighted_count(&t.body, multiplier, fn_costs);
                total += weighted_count(&t.orelse, multiplier, fn_costs);
                total += weighted_count(&t.finalbody, multiplier, fn_costs);
            }
            // Function definitions are NOT recursed — costs come from fn_costs.
            Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_) => {}
            Stmt::Expr(e) => {
                let mut counter = ExprCounter { count: 0, fn_costs };
                counter.visit_expr(&e.value);
                total += counter.count * multiplier;
            }
            Stmt::Assign(a) => {
                let mut counter = ExprCounter { count: 0, fn_costs };
                counter.visit_expr(&a.value);
                total += counter.count * multiplier;
            }
            _ => {}
        }
    }
    total
}

// ── Per-function cost computation ─────────────────────────────────────────────

fn body_explode_cost(body: &[Stmt], fn_costs: &HashMap<String, i64>) -> i64 {
    weighted_count(body, 1, fn_costs)
}

/// Build a `function_name → weighted_explode_cost` map for all top-level
/// function definitions in `stmts`.  Uses iterative convergence (up to 10
/// rounds) to handle transitive calls.
///
/// `pub(crate)` so `checker.rs` can call this during the global pre-pass.
pub(crate) fn build_fn_explode_costs(
    stmts: &[Stmt],
    seed: &HashMap<String, i64>,
) -> HashMap<String, i64> {
    let mut fn_bodies: Vec<(String, &[Stmt])> = vec![];
    for stmt in stmts {
        match stmt {
            Stmt::FunctionDef(f) => fn_bodies.push((f.name.to_string(), &f.body)),
            Stmt::AsyncFunctionDef(f) => fn_bodies.push((f.name.to_string(), &f.body)),
            _ => {}
        }
    }

    let mut fn_costs = seed.clone();
    for _ in 0..10 {
        let mut changed = false;
        for (name, body) in &fn_bodies {
            let new_cost = body_explode_cost(body, &fn_costs);
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

    fn_bodies
        .iter()
        .filter_map(|(name, _)| fn_costs.get(name).map(|&c| (name.clone(), c)))
        .collect()
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let mut fn_costs = config.global_fn_explode_costs.clone();
    fn_costs.extend(build_fn_explode_costs(stmts, &fn_costs.clone()));

    let weighted = weighted_count(stmts, 1, &fn_costs);
    if weighted <= config.explode_threshold as i64 {
        return vec![];
    }

    let mut collector = OccurrenceCollector {
        source, file, index,
        severity: config.severity_of(ID),
        fn_costs: &fn_costs,
        occurrences: vec![],
    };
    for s in stmts {
        collector.visit_stmt(s);
    }
    collector.occurrences
}
