// S002: Join without a broadcast or merge hint
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{chain_has_method, method_violation},
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "S002";

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
                if attr.attr.as_str() == "join" {
                    // Skip str.join(...) — receiver is a string literal
                    if matches!(attr.value.as_ref(), Expr::Constant(c) if matches!(c.value, rustpython_parser::ast::Constant::Str(_))) {
                        walk_expr(self, expr);
                        return;
                    }
                    // Check that neither the left DataFrame nor the first
                    // argument (right DataFrame) has a .hint() call.
                    let left_has_hint = chain_has_method(attr.value.as_ref(), "hint");
                    let right_has_hint = call.args.first().map_or(false, |a| chain_has_method(a, "hint"));
                    if !left_has_hint && !right_has_hint {
                        self.violations.push(method_violation(
                            attr, "join", self.source, self.file, self.index,
                            self.severity, ID,
                        ));
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
