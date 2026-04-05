//! D009: Avoid using `.count()` as a boolean truth value — use `.isEmpty()` instead.
//!
//! Detects `.count()` used directly as a condition:
//!   - `if df.count():`              — should be `if not df.isEmpty():`
//!   - `if not df.count():`          — should be `if df.isEmpty():`
//!   - `if x and df.count():`        — count in a boolean `and`/`or` chain
//!   - `if x and not df.count():`    — negated count in a boolean chain
//!   - `while df.count():`           — same patterns in while conditions
use rustpython_parser::ast::{Expr, Stmt, UnaryOp};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_stmt},
};

const ID: &str = "D009";

/// Returns `true` when `expr` is a `.count()` call on a non-literal receiver.
fn is_count_call(expr: &Expr) -> bool {
    if let Expr::Call(call) = expr
        && let Expr::Attribute(attr) = call.func.as_ref()
        && attr.attr.as_str() == "count"
    {
        return !matches!(
            attr.value.as_ref(),
            Expr::Constant(_) | Expr::List(_) | Expr::Tuple(_) | Expr::Set(_) | Expr::Dict(_)
        );
    }
    false
}

/// Recursively walk a boolean-context expression and collect every `.count()`
/// call that is used directly as a truth value (not inside a comparison).
///
/// Recurses into:
/// - `not <expr>`             — the operand is still in boolean context
/// - `<expr> and/or <expr>`   — each operand is in boolean context
///
/// Does NOT recurse into comparisons (`count() == 0`) or arbitrary calls,
/// so D006/D007 patterns are not double-reported.
fn collect_boolean_counts<'a>(expr: &'a Expr, found: &mut Vec<&'a Expr>) {
    match expr {
        _ if is_count_call(expr) => {
            found.push(expr);
        }
        Expr::UnaryOp(u) if matches!(u.op, UnaryOp::Not) => {
            collect_boolean_counts(&u.operand, found);
        }
        Expr::BoolOp(b) => {
            for val in &b.values {
                collect_boolean_counts(val, found);
            }
        }
        _ => {}
    }
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        let test = match stmt {
            Stmt::If(s) => Some(s.test.as_ref()),
            Stmt::While(s) => Some(s.test.as_ref()),
            _ => None,
        };

        if let Some(test_expr) = test {
            let mut found = vec![];
            collect_boolean_counts(test_expr, &mut found);
            for count_expr in found {
                if let Expr::Call(c) = count_expr
                    && let Expr::Attribute(attr) = c.func.as_ref()
                {
                    self.violations.push(method_violation(
                        attr,
                        "count",
                        self.source,
                        self.file,
                        self.index,
                        self.severity,
                        ID,
                    ));
                }
            }
        }

        walk_stmt(self, stmt);
    }
}

/// Scan `stmts` for `.count()` used as a boolean condition and return a violation for each.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        violations: vec![],
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
