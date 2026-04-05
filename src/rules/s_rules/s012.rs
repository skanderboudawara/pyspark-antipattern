// S012: Avoid inner join followed by direct filter; prefer leftSemi join
use rustpython_parser::ast::{Constant, Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "S012";

/// Returns true if the join call uses "inner" as its how argument
/// (default is inner when not specified).
fn is_inner_join(call_args: &[Expr], call_keywords: &[rustpython_parser::ast::Keyword]) -> bool {
    // Explicit "inner" as third positional arg or how="inner" keyword
    if let Some(how_arg) = call_args.get(2)
        && let Expr::Constant(c) = how_arg
            && let Constant::Str(s) = &c.value {
                return s == "inner";
            }
    for kw in call_keywords {
        if kw.arg.as_ref().is_some_and(|a| a.as_str() == "how")
            && let Expr::Constant(c) = &kw.value
                && let Constant::Str(s) = &c.value {
                    return s == "inner";
                }
    }
    // No explicit how → defaults to inner
    true
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        // Pattern: filter()/where() called on result of inner join()
        if let Expr::Call(outer_call) = expr
            && let Expr::Attribute(outer_attr) = outer_call.func.as_ref()
                && matches!(outer_attr.attr.as_str(), "filter" | "where")
                    && let Expr::Call(inner_call) = outer_attr.value.as_ref()
                        && let Expr::Attribute(inner_attr) = inner_call.func.as_ref() {
                            let inner_receiver_is_str = matches!(
                                inner_attr.value.as_ref(),
                                Expr::Constant(c) if matches!(c.value, Constant::Str(_))
                            );
                            if inner_attr.attr.as_str() == "join"
                                && !inner_receiver_is_str
                                && is_inner_join(&inner_call.args, &inner_call.keywords)
                            {
                                self.violations.push(method_violation(
                                    outer_attr, outer_attr.attr.as_str(),
                                    self.source, self.file, self.index,
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
