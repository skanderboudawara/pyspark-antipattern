mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::{d_rules::*, f_rules::*, s_rules::*};

// ── Multiline with parentheses ────────────────────────────────────────────────

#[test]
fn collect_multiline_parens() {
    // violation should point to the line where .collect() actually appears
    let src = "result = (\n    df\n    .collect()\n)";
    let v = check(d001::check, src);
    assert_violation(&v, "D001", 3); // line 3 = "    .collect()"
}

#[test]
fn collect_multiline_backslash() {
    let src = "result = df \\\n    .collect()";
    let v = check(d001::check, src);
    assert_violation(&v, "D001", 2);
}

#[test]
fn f005_multiline_chain() {
    let src = "df\\\n    .withColumn('a', col('x'))\\\n    .withColumn('b', col('y'))";
    assert_violation(&check(f005::check, src), "F005", 3);
}

// ── Extra spaces ──────────────────────────────────────────────────────────────

#[test]
fn collect_extra_spaces() {
    // Python parser handles spaces around dots and calls
    let src = "df.collect( )";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn collect_inline_comment() {
    let src = "result = df.collect()  # get all rows";
    assert_violation(&check(d001::check, src), "D001", 1);
}

// ── noqa suppression on correct line ──────────────────────────────────────────

#[test]
fn noqa_suppresses_on_correct_line() {
    use pyspark_antipattern::{config::Config, line_index::LineIndex, noqa, rules::RuleFn};
    use rustpython_parser::{Mode, ast::Mod, parse};

    fn check_with_noqa(rule_fn: RuleFn, source: &str) -> Vec<pyspark_antipattern::violation::Violation> {
        let parsed = parse(source, Mode::Module, "<test>").unwrap();
        let stmts = match parsed {
            Mod::Module(m) => m.body,
            _ => vec![],
        };
        let index = LineIndex::new(source);
        let suppressions = noqa::parse_suppressions(source);
        let violations = rule_fn(&stmts, source, "<test>", &Config::default(), &index);
        noqa::filter_suppressed(violations, &suppressions)
    }

    // suppressed on same line
    let src = "result = df.collect()  # noqa: pap: D001";
    assert!(
        check_with_noqa(d001::check, src).is_empty(),
        "noqa on same line should suppress"
    );

    // noqa on wrong line should NOT suppress
    let src2 = "# noqa: pap: D001\nresult = df.collect()";
    assert!(
        !check_with_noqa(d001::check, src2).is_empty(),
        "noqa on different line should not suppress"
    );
}

// ── Windows line endings (\r\n) ───────────────────────────────────────────────

#[test]
fn collect_crlf_line_endings() {
    let src = "x = 1\r\nresult = df.collect()\r\n";
    let v = check(d001::check, src);
    assert_violation(&v, "D001", 2);
}

// ── Nested / deeply chained ───────────────────────────────────────────────────

#[test]
fn collect_inside_if_condition() {
    let src = "if df.collect(): pass";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn collect_inside_function() {
    let src = "def process():\n    return df.collect()";
    assert_violation(&check(d001::check, src), "D001", 2);
}

#[test]
fn violation_inside_with_block() {
    let src = "with spark.session() as s:\n    df.show()";
    assert_violation(&check(d003::check, src), "D003", 2);
}

// ── S007 coalesce(1) vs coalesce(2) ──────────────────────────────────────────

#[test]
fn s007_coalesce_1_fires() {
    assert_violation(&check(s007::check, "df.coalesce(1)"), "S007", 1);
}
#[test]
fn s007_coalesce_2_no_fire() {
    assert_no_violation(&check(s007::check, "df.coalesce(2)"), "S007");
}
