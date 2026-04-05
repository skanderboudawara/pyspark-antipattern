//! D004: Avoid `.count()` on large DataFrames — triggers a full scan and shuffle
//! to count every row, which can be extremely expensive at scale.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "D004";

/// Returns `true` when `expr` is a non-DataFrame literal (constant, list, tuple, set, or dict).
/// Used to suppress false positives such as `"hello".count("l")`.
fn is_non_df_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Constant(_) | Expr::List(_) | Expr::Tuple(_) | Expr::Set(_) | Expr::Dict(_)
    )
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "count"
        {
            // Skip str/list/tuple/set literals — e.g. "hello".count("l")
            if is_non_df_literal(attr.value.as_ref()) {
                walk_expr(self, expr);
                return;
            }
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
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for `.count()` calls on non-literal receivers and return a violation for each.
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
