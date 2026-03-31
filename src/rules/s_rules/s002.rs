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

/// Walk the receiver's attribute chain to its root Name and check whether it
/// is a known stdlib/utility module that is not a Spark DataFrame.
/// e.g. `os.path.join(...)` → root is "os" → not a DataFrame.
fn receiver_is_non_dataframe(expr: &Expr) -> bool {
    // String literal receiver: `",".join(...)` — handled separately but
    // included here for completeness.
    if matches!(expr, Expr::Constant(c) if matches!(c.value, rustpython_parser::ast::Constant::Str(_))) {
        return true;
    }
    // Walk attribute chain to the root Name.
    let mut current = expr;
    loop {
        match current {
            Expr::Attribute(a) => current = a.value.as_ref(),
            Expr::Name(n) => {
                return matches!(
                    n.id.as_str(),
                    "os" | "sys" | "pathlib" | "str" | "bytes"
                    | "urllib" | "posixpath" | "ntpath" | "shutil"
                    | "Path" | "PurePath" | "PosixPath" | "WindowsPath"
                );
            }
            // Call result (e.g. `Path(...).join(...)`) — could be legit or not;
            // we don't flag these to avoid false negatives on DataFrame chains.
            _ => return false,
        }
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
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            if let Expr::Attribute(attr) = call.func.as_ref() {
                if attr.attr.as_str() == "join" && !receiver_is_non_dataframe(attr.value.as_ref()) {
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
