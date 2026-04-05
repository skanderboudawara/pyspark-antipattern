// F001: Avoid chaining withColumn() and withColumnRenamed() together
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{method_chain, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F001";

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
        {
            let name = attr.attr.as_str();
            if name == "withColumn" || name == "withColumnRenamed" {
                let chain = method_chain(expr);
                let has_with_col = chain.contains(&"withColumn");
                let has_with_col_renamed = chain.contains(&"withColumnRenamed");
                if has_with_col && has_with_col_renamed {
                    let end: u32 = attr.range.end().into();
                    let start = end.saturating_sub(name.len() as u32);
                    let (line, col) = self.index.line_col(start);
                    if self.seen.insert((line, col)) {
                        self.violations.push(method_violation(
                            attr,
                            name,
                            self.source,
                            self.file,
                            self.index,
                            self.severity,
                            ID,
                        ));
                    }
                }
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
