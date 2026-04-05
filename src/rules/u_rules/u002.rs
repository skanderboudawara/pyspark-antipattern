//! U002: Avoid UDFs that return `ArrayType` — use built-in array functions
//! (`array_distinct`, `transform`, `filter`, etc.) for Catalyst-optimised processing.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_stmt},
};

const ID: &str = "U002";

fn decorator_returns_array_type(expr: &Expr) -> bool {
    if let Expr::Call(call) = expr {
        let is_udf = match call.func.as_ref() {
            Expr::Name(n) => n.id.as_str() == "udf",
            Expr::Attribute(a) => a.attr.as_str() == "udf",
            _ => false,
        };
        if !is_udf {
            return false;
        }
        if let Some(first) = call.args.first()
            && is_array_type_call(first)
        {
            return true;
        }
        for kw in &call.keywords {
            if kw.arg.as_ref().is_some_and(|a| a.as_str() == "returnType") && is_array_type_call(&kw.value) {
                return true;
            }
        }
    }
    false
}

fn is_array_type_call(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr {
        match c.func.as_ref() {
            Expr::Name(n) => return n.id.as_str() == "ArrayType",
            Expr::Attribute(a) => return a.attr.as_str() == "ArrayType",
            _ => {}
        }
    }
    false
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
                    if decorator_returns_array_type(decorator) {
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
                    if decorator_returns_array_type(decorator) {
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

/// Scan `stmts` for UDFs decorated with `@udf(returnType=ArrayType(...))` and flag each.
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
