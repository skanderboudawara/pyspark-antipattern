//! D006: Avoid `df.count() == 0` — use `.isEmpty()` for a more efficient
//! emptiness check that avoids a full-scan count.
use rustpython_parser::ast::{CmpOp, Constant, Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "D006";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

fn is_count_call(expr: &Expr) -> bool {
    if let Expr::Call(call) = expr
        && let Expr::Attribute(attr) = call.func.as_ref()
        && attr.attr.as_str() == "count"
    {
        // Skip str/list/tuple/set literals — e.g. "hello".count("x") == 0
        return !matches!(
            attr.value.as_ref(),
            Expr::Constant(_) | Expr::List(_) | Expr::Tuple(_) | Expr::Set(_) | Expr::Dict(_)
        );
    }
    false
}

fn is_zero(expr: &Expr) -> bool {
    if let Expr::Constant(c) = expr
        && let Constant::Int(n) = &c.value
    {
        return n.to_string() == "0";
    }
    false
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        // Pattern: Compare { left: count(), ops: [Eq | NotEq], comparators: [0] }
        if let Expr::Compare(cmp) = expr
            && cmp.ops.len() == 1
            && matches!(cmp.ops[0], CmpOp::Eq | CmpOp::NotEq)
            && cmp.comparators.len() == 1
        {
            let (lhs, rhs) = (cmp.left.as_ref(), &cmp.comparators[0]);
            if (is_count_call(lhs) && is_zero(rhs)) || (is_zero(lhs) && is_count_call(rhs)) {
                self.violations.push(expr_violation(
                    expr,
                    "count() == 0".len(),
                    self.source,
                    self.file,
                    self.index,
                    self.severity,
                    ID,
                ));
            }
        }
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for `count() == 0` / `0 == count()` comparisons and return a violation for each.
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
