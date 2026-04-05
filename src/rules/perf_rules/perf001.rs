// PERF001: Avoid .rdd.collect() — use .toPandas() instead
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "PERF001";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        // Detect .collect() whose receiver chain contains .rdd
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
                && attr.attr.as_str() == "collect"
                    && (crate::rules::utils::chain_has_method(attr.value.as_ref(), "rdd")
                        || matches!(
                            attr.value.as_ref(),
                            Expr::Attribute(a) if a.attr.as_str() == "rdd"
                        ))
                    {
                        self.violations.push(method_violation(
                            attr, "collect", self.source, self.file,
                            self.index, self.severity, ID,
                        ));
                    }
        walk_expr(self, expr);
    }
}

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let mut v = Check {
        source, file, index,
        severity: config.severity_of(ID),
        violations: vec![],
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
