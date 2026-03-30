// U001: Avoid UDFs that return StringType
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{walk_stmt, Visitor},
};

const ID: &str = "U001";

fn decorator_returns_string_type(expr: &Expr) -> bool {
    // Matches: @udf(returnType=StringType()) or @udf(StringType())
    if let Expr::Call(call) = expr {
        let is_udf = match call.func.as_ref() {
            Expr::Name(n) => n.id.as_str() == "udf",
            Expr::Attribute(a) => a.attr.as_str() == "udf",
            _ => false,
        };
        if !is_udf {
            return false;
        }
        // Check positional arg
        if let Some(first) = call.args.first() {
            if is_string_type_call(first) {
                return true;
            }
        }
        // Check keyword arg returnType
        for kw in &call.keywords {
            if kw.arg.as_ref().map_or(false, |a| a.as_str() == "returnType") {
                if is_string_type_call(&kw.value) {
                    return true;
                }
            }
        }
    }
    false
}

fn is_string_type_call(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr {
        match c.func.as_ref() {
            Expr::Name(n) => return n.id.as_str() == "StringType",
            Expr::Attribute(a) => return a.attr.as_str() == "StringType",
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
                    if decorator_returns_string_type(decorator) {
                        self.violations.push(expr_violation(
                            decorator, "udf".len(),
                            self.source, self.file, self.index, self.severity, ID,
                        ));
                    }
                }
                for s in &f.body { self.visit_stmt(s); }
            }
            Stmt::AsyncFunctionDef(f) => {
                for decorator in &f.decorator_list {
                    if decorator_returns_string_type(decorator) {
                        self.violations.push(expr_violation(
                            decorator, "udf".len(),
                            self.source, self.file, self.index, self.severity, ID,
                        ));
                    }
                }
                for s in &f.body { self.visit_stmt(s); }
            }
            _ => walk_stmt(self, stmt),
        }
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
