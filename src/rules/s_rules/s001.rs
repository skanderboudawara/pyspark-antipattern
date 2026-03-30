// S001: Missing .coalesce() or .repartition() after .union() / .unionByName()
use rustpython_parser::ast::{Expr, Stmt};
use std::collections::HashSet;

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "S001";

/// Mark all union/unionByName call start-offsets that are directly followed
/// by coalesce or repartition in the same chain.
fn mark_ok_unions(expr: &Expr, ok: &mut HashSet<u32>) {
    if let Expr::Call(call) = expr {
        if let Expr::Attribute(attr) = call.func.as_ref() {
            let name = attr.attr.as_str();
            if name == "coalesce" || name == "repartition" {
                // Find the union/unionByName inside the chain below this call.
                find_union_offsets(attr.value.as_ref(), ok);
            }
            // Recurse into the function expression and args
            mark_ok_unions(attr.value.as_ref(), ok);
        }
        for arg in &call.args {
            mark_ok_unions(arg, ok);
        }
    }
}

fn find_union_offsets(expr: &Expr, set: &mut HashSet<u32>) {
    if let Expr::Call(call) = expr {
        if let Expr::Attribute(attr) = call.func.as_ref() {
            if matches!(attr.attr.as_str(), "union" | "unionByName") {
                set.insert(call.range.start().into());
                return;
            }
            // Keep looking down
            find_union_offsets(attr.value.as_ref(), set);
        }
    }
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
    ok_unions: HashSet<u32>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            if let Expr::Attribute(attr) = call.func.as_ref() {
                if matches!(attr.attr.as_str(), "union" | "unionByName") {
                    let offset: u32 = call.range.start().into();
                    if !self.ok_unions.contains(&offset) {
                        self.violations.push(method_violation(
                            attr, attr.attr.as_str(), self.source, self.file,
                            self.index, self.severity, ID,
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
    // First pass: collect union offsets that have coalesce/repartition applied.
    let mut ok_unions: HashSet<u32> = HashSet::new();
    struct FirstPass<'a> {
        ok: &'a mut HashSet<u32>,
    }
    impl<'a> Visitor for FirstPass<'a> {
        fn visit_expr(&mut self, expr: &Expr) {
            mark_ok_unions(expr, self.ok);
            walk_expr(self, expr);
        }
    }
    let mut fp = FirstPass { ok: &mut ok_unions };
    for s in stmts { fp.visit_stmt(s); }

    // Second pass: flag remaining unions.
    let mut v = Check {
        source, file, index,
        severity: config.severity_of(ID),
        violations: vec![],
        ok_unions,
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
