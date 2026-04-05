// S002: Join without a broadcast or merge hint
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{chain_has_method, is_non_dataframe_receiver, method_violation},
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
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
                && attr.attr.as_str() == "join" && !is_non_dataframe_receiver(attr.value.as_ref()) {
                    // Check that neither the left DataFrame nor the first
                    // argument (right DataFrame) has a .hint() call.
                    let left_has_hint = chain_has_method(attr.value.as_ref(), "hint");
                    let right_has_hint = call.args.first().is_some_and(|a| chain_has_method(a, "hint"));
                    if !left_has_hint && !right_has_hint {
                        self.violations.push(method_violation(
                            attr, "join", self.source, self.file, self.index,
                            self.severity, ID,
                        ));
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
