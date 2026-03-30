// S011: Join without join conditions (no `on` argument) causes a nested-loop scan
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "S011";

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
                    // Skip str.join(...) — the receiver is a string literal or variable
                    // e.g. " ".join(...) or ",".join(...)
                    let receiver_is_str = matches!(
                        attr.value.as_ref(),
                        Expr::Constant(c) if matches!(c.value, rustpython_parser::ast::Constant::Str(_))
                    );
                    if receiver_is_str {
                        walk_expr(self, expr);
                        return;
                    }

                    // Flag if only one positional argument (the right DF) and no keywords.
                    // df.join(other)            → no condition → Cartesian
                    // df.join(other, "id")      → has condition → OK
                    // df.join(other, on="id")   → has condition via keyword → OK
                    let no_on_arg = call.args.len() <= 1;
                    let no_on_kw = !call.keywords.iter().any(|k| {
                        k.arg.as_ref().map_or(false, |a| a.as_str() == "on")
                    });
                    if no_on_arg && no_on_kw {
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
