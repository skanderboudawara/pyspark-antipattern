// U003: Avoid UDFs in general; prefer Spark built-in functions
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_stmt},
};

const ID: &str = "U003";

fn is_udf_decorator(expr: &Expr) -> bool {
    match expr {
        Expr::Name(n) => n.id.as_str() == "udf" || n.id.as_str() == "pandas_udf",
        Expr::Attribute(a) => {
            matches!(a.attr.as_str(), "udf" | "pandas_udf")
        }
        Expr::Call(c) => match c.func.as_ref() {
            Expr::Name(n) => n.id.as_str() == "udf" || n.id.as_str() == "pandas_udf",
            Expr::Attribute(a) => matches!(a.attr.as_str(), "udf" | "pandas_udf"),
            _ => false,
        },
        _ => false,
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
        match stmt {
            Stmt::FunctionDef(f) => {
                for decorator in &f.decorator_list {
                    if is_udf_decorator(decorator) {
                        self.violations.push(expr_violation(
                            decorator,
                            "udf".len(),
                            self.source,
                            self.file,
                            self.index,
                            self.severity,
                            ID,
                        ));
                    }
                }
                for s in &f.body {
                    self.visit_stmt(s);
                }
            }
            Stmt::AsyncFunctionDef(f) => {
                for decorator in &f.decorator_list {
                    if is_udf_decorator(decorator) {
                        self.violations.push(expr_violation(
                            decorator,
                            "udf".len(),
                            self.source,
                            self.file,
                            self.index,
                            self.severity,
                            ID,
                        ));
                    }
                }
                for s in &f.body {
                    self.visit_stmt(s);
                }
            }
            _ => walk_stmt(self, stmt),
        }
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
