// F013: Avoid reserved column names (__ prefix + __ suffix)
// Columns like __index__ are reserved by pandas API on Spark for internal use.
use rustpython_parser::ast::{Constant, Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{expr_violation, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F013";

fn is_reserved_name(s: &str) -> bool {
    s.starts_with("__") && s.ends_with("__") && s.len() > 4
}

fn reserved_str_arg(expr: &Expr) -> Option<&str> {
    if let Expr::Constant(c) = expr
        && let Constant::Str(s) = &c.value
        && is_reserved_name(s)
    {
        return Some(s.as_str());
    }
    None
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
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
        {
            match attr.attr.as_str() {
                // withColumn("__name__", expr) — check first arg
                "withColumn" => {
                    if let Some(arg) = call.args.first()
                        && reserved_str_arg(arg).is_some()
                    {
                        self.violations.push(expr_violation(
                            arg,
                            1,
                            self.source,
                            self.file,
                            self.index,
                            self.severity,
                            ID,
                        ));
                    }
                }
                // withColumnRenamed("old", "__new__") — check second arg
                "withColumnRenamed" => {
                    if let Some(arg) = call.args.get(1)
                        && reserved_str_arg(arg).is_some()
                    {
                        self.violations.push(expr_violation(
                            arg,
                            1,
                            self.source,
                            self.file,
                            self.index,
                            self.severity,
                            ID,
                        ));
                    }
                }
                // col("x").alias("__name__") — check first arg
                "alias" => {
                    if let Some(arg) = call.args.first()
                        && reserved_str_arg(arg).is_some()
                    {
                        self.violations.push(method_violation(
                            attr,
                            "alias",
                            self.source,
                            self.file,
                            self.index,
                            self.severity,
                            ID,
                        ));
                    }
                }
                _ => {}
            }
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
