// S005: .repartition() with fewer partitions than the Spark default (200)
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{const_int, method_violation},
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "S005";
const SPARK_DEFAULT_PARTITIONS: i64 = 200;

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            if let Expr::Attribute(attr) = call.func.as_ref() {
                if attr.attr.as_str() == "repartition" {
                    if let Some(n) = call.args.first().and_then(|a| const_int(a)) {
                        if n > 0 && n < SPARK_DEFAULT_PARTITIONS {
                            self.violations.push(method_violation(
                                attr, "repartition", self.source, self.file,
                                self.index, self.severity, ID,
                            ));
                        }
                    }
                }
            }
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
