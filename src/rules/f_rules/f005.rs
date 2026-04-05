// F005: Avoid stacking multiple withColumn() calls; use withColumns()
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{consecutive_method_depth, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F005";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
    seen: std::collections::HashSet<(usize, usize)>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "withColumn"
            && consecutive_method_depth(expr, "withColumn") >= 2
        {
            let end: u32 = attr.range.end().into();
            let start = end.saturating_sub("withColumn".len() as u32);
            let (line, col) = self.index.line_col(start, self.source);
            if self.seen.insert((line, col)) {
                self.violations.push(method_violation(
                    attr,
                    "withColumn",
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

pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        violations: vec![],
        seen: std::collections::HashSet::new(),
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
