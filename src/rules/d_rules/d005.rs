//! D005: Avoid `.rdd.isEmpty()` — use `.isEmpty()` directly on the DataFrame
//! to avoid the unnecessary RDD conversion overhead.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "D005";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        // Pattern: Call { func: Attribute { value: Attribute { attr: "rdd" }, attr: "isEmpty" } }
        if let Expr::Call(call) = expr
            && let Expr::Attribute(outer) = call.func.as_ref()
            && outer.attr.as_str() == "isEmpty"
            && let Expr::Attribute(inner) = outer.value.as_ref()
            && inner.attr.as_str() == "rdd"
        {
            self.violations.push(method_violation(
                outer,
                "isEmpty",
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

/// Scan `stmts` for the `.rdd.isEmpty()` pattern and return a violation for each occurrence.
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
