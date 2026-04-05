// P001: .toPandas() without enabling Arrow optimization
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "P001";
const ARROW_FLAG: &str = "spark.sql.execution.arrow.pyspark.enabled";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
    arrow_enabled: bool,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
                && attr.attr.as_str() == "toPandas" && !self.arrow_enabled {
                    self.violations.push(method_violation(
                        attr, "toPandas", self.source, self.file, self.index,
                        self.severity, ID,
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
    // Heuristic: check if arrow is configured anywhere in the source file.
    let arrow_enabled = source.contains(ARROW_FLAG);

    let mut v = Check {
        source, file, index,
        severity: config.severity_of(ID),
        violations: vec![],
        arrow_enabled,
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
