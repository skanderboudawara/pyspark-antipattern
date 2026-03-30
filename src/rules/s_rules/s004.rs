// S004: Too many .distinct() operations in one file
// Loop-aware: a .distinct() inside range(N) counts as N occurrences;
// inside a while loop it counts as 99 (assumed iterations).
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{for_loop_iters, method_violation},
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "S004";
const WHILE_ASSUMED_ITERS: i64 = 99;

/// Counts `.distinct()` calls in an expression subtree.
struct ExprCounter {
    count: i64,
}
impl Visitor for ExprCounter {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            if let Expr::Attribute(attr) = call.func.as_ref() {
                if attr.attr.as_str() == "distinct" {
                    self.count += 1;
                }
            }
        }
        walk_expr(self, expr);
    }
}

/// Collects violation objects for each `.distinct()` call in a subtree.
struct OccurrenceCollector<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    occurrences: Vec<Violation>,
}
impl<'a> Visitor for OccurrenceCollector<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            if let Expr::Attribute(attr) = call.func.as_ref() {
                if attr.attr.as_str() == "distinct" {
                    self.occurrences.push(method_violation(
                        attr, "distinct", self.source, self.file, self.index,
                        self.severity, ID,
                    ));
                }
            }
        }
        walk_expr(self, expr);
    }
}

/// Walk statements and accumulate a weighted distinct() count.
/// `multiplier` is the product of enclosing loop iteration counts.
fn weighted_count(stmts: &[Stmt], multiplier: i64) -> i64 {
    let mut total = 0i64;
    for stmt in stmts {
        match stmt {
            Stmt::For(f) => {
                let iters = for_loop_iters(&f.iter).unwrap_or(i64::MAX / multiplier);
                let m = multiplier.saturating_mul(iters);
                total += weighted_count(&f.body, m);
                total += weighted_count(&f.orelse, multiplier);
            }
            Stmt::While(w) => {
                let m = multiplier.saturating_mul(WHILE_ASSUMED_ITERS);
                total += weighted_count(&w.body, m);
                total += weighted_count(&w.orelse, multiplier);
            }
            Stmt::If(i) => {
                total += weighted_count(&i.body, multiplier);
                total += weighted_count(&i.orelse, multiplier);
            }
            Stmt::With(w) => total += weighted_count(&w.body, multiplier),
            Stmt::Try(t) => {
                total += weighted_count(&t.body, multiplier);
                total += weighted_count(&t.orelse, multiplier);
                total += weighted_count(&t.finalbody, multiplier);
            }
            Stmt::FunctionDef(f) => total += weighted_count(&f.body, multiplier),
            Stmt::Expr(e) => {
                let mut counter = ExprCounter { count: 0 };
                counter.visit_expr(&e.value);
                total += counter.count * multiplier;
            }
            Stmt::Assign(a) => {
                let mut counter = ExprCounter { count: 0 };
                counter.visit_expr(&a.value);
                total += counter.count * multiplier;
            }
            _ => {}
        }
    }
    total
}

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let weighted = weighted_count(stmts, 1);
    if weighted <= config.distinct_threshold as i64 {
        return vec![];
    }

    // Emit a violation for every actual call site.
    let mut collector = OccurrenceCollector {
        source, file, index,
        severity: config.severity_of(ID),
        occurrences: vec![],
    };
    for s in stmts { collector.visit_stmt(s); }
    collector.occurrences
}
