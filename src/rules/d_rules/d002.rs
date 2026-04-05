// D002: Avoid accessing .rdd
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    violation::{RuleId, Violation},
    visitor::{Visitor, walk_expr},
};

const ID: &str = "D002";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Attribute(attr) = expr
            && attr.attr.as_str() == "rdd"
        {
            let end: u32 = attr.range.end().into();
            let start = end.saturating_sub("rdd".len() as u32);
            let (line, col) = self.index.line_col(start, self.source);
            let source_line = self.index.line_text(self.source, line).to_string();
            self.violations.push(Violation {
                rule_id: RuleId(ID.to_string()),
                severity: self.severity,
                impact: crate::violation::Impact::Low,
                file: self.file.to_string(),
                line,
                col,
                source_line,
                span_len: 3, // "rdd"
            });
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
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
