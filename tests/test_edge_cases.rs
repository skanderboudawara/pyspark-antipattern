mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::{
    line_index::LineIndex,
    rules::{d_rules::*, f_rules::*, s_rules::*},
};

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

// ── Fix 2: S001 set-variable receiver tracking ────────────────────────────────

#[test]
fn s001_no_fire_set_variable() {
    // x is assigned from set() — x.union(y) should not fire S001
    let src = "x = set(a)\ny = x.union(b)";
    assert_no_violation(&check(s001::check, src), "S001");
}

#[test]
fn s001_no_fire_frozenset_variable() {
    let src = "x = frozenset([1, 2, 3])\nresult = x.union(frozenset([4, 5]))";
    assert_no_violation(&check(s001::check, src), "S001");
}

#[test]
fn s001_fires_non_set_variable() {
    // df is not a set — df.union(df2) should still fire
    let src = "df = spark.read.parquet('a')\nresult = df.union(df2)";
    assert_violation(&check(s001::check, src), "S001", 2);
}

// ── Fix 3: S004 / S008 two-level lookup (no global clone) ─────────────────────

#[test]
fn s004_helper_fn_cost_still_tracked() {
    // A helper that calls .distinct() multiple times should still be counted
    // even after the two-level lookup refactor.
    let src = "def helper(df):\n    a = df.distinct()\n    b = a.distinct()\n    c = b.distinct()\n    d = c.distinct()\n    e = d.distinct()\n    return e.distinct()\nhelper(df1)\nhelper(df2)\nhelper(df3)\n";
    // helper has cost 6; called 3 times → total 18 > default threshold 5
    assert_violation(&check(s004::check, src), "S004", 8);
}

#[test]
fn s008_helper_fn_cost_still_tracked() {
    use pyspark_antipattern::rules::s_rules::s008;
    let src = "def helper(df):\n    a = df.select(explode(col('a')))\n    b = a.select(explode(col('b')))\n    c = b.select(explode(col('c')))\n    return c.select(explode(col('d')))\nhelper(df1)\nhelper(df2)\n";
    // helper has cost 4; called 2 times → total 8 > default threshold 3
    assert_violation(&check(s008::check, src), "S008", 6);
}

// ── Fix 4: Unicode column numbers ─────────────────────────────────────────────

#[test]
fn line_col_ascii_column_is_correct() {
    // Pure ASCII: byte offset == char offset, both should give col = 5
    let source = "abcd.collect()";
    let index = LineIndex::new(source);
    let (line, col) = index.line_col(5, source); // offset 5 = 'c' in 'collect'
    assert_eq!(line, 1);
    assert_eq!(col, 6); // 1-based
}

#[test]
fn line_col_unicode_column_counts_chars_not_bytes() {
    // "🔥" is 4 bytes but 1 char; column after it should be 2, not 5
    let source = "🔥.collect()";
    let index = LineIndex::new(source);
    // "🔥" occupies bytes 0..4; the dot is at byte offset 4
    let (line, col) = index.line_col(4, source);
    assert_eq!(line, 1);
    assert_eq!(col, 2); // 1 char (the emoji) + 1 = col 2
}

#[test]
fn line_col_cjk_column_counts_chars_not_bytes() {
    // "中" is 3 bytes in UTF-8 but 1 char
    let source = "中文.collect()";
    let index = LineIndex::new(source);
    // "中" = bytes 0..3, "文" = bytes 3..6, dot at byte 6
    let (line, col) = index.line_col(6, source);
    assert_eq!(line, 1);
    assert_eq!(col, 3); // 2 CJK chars + 1 = col 3
}

#[test]
fn line_col_multiline_unicode() {
    let source = "x = 1\n🔥y = df.collect()";
    let index = LineIndex::new(source);
    // Line 2 starts at byte 6. "🔥" = 4 bytes, "y" at byte 10.
    // "collect" method in the call starts well after that.
    let (line, _col) = index.line_col(6, source); // start of line 2
    assert_eq!(line, 2);
}

// ── Fix 5: File-level noqa suppression ───────────────────────────────────────

#[test]
fn noqa_file_suppresses_all_violations() {
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

    // `# noqa: pap: FILE` on any line suppresses the entire file
    let src = "# noqa: pap: FILE\nresult = df.collect()\nresult2 = df.collect()";
    assert!(
        check_with_noqa(d001::check, src).is_empty(),
        "file-level noqa should suppress all violations"
    );

    // Without FILE keyword, only the annotated line is suppressed
    let src2 = "result = df.collect()  # noqa: pap: D001\nresult2 = df.collect()";
    let v = check_with_noqa(d001::check, src2);
    assert_eq!(v.len(), 1, "line-level noqa should suppress only that line");
    assert_eq!(v[0].line, 2);
}

#[test]
fn noqa_file_keyword_mid_file() {
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

    // FILE keyword can appear anywhere in the file (e.g. end of a header block)
    let src = "import pyspark\n# noqa: pap: FILE\nresult = df.collect()";
    assert!(check_with_noqa(d001::check, src).is_empty());
}

// ── Fix 6: visitor traversal gaps ────────────────────────────────────────────

#[test]
fn d001_in_listcomp_iterator() {
    // violation in the `for x in <here>` part of a list comprehension
    let src = "rows = [x for x in df.collect()]";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_listcomp_filter() {
    // violation in the `if <here>` filter of a list comprehension
    let src = "rows = [x for x in items if df.collect()]";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_dictcomp_value() {
    // violation in the value expression of a dict comprehension
    let src = "d = {k: df.collect() for k in keys}";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_dictcomp_iterator() {
    // violation in the iterator of a dict comprehension
    let src = "d = {k: v for k, v in df.collect()}";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_genexp_iterator() {
    // violation in the iterator of a generator expression
    let src = "total = sum(x for x in df.collect())";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_with_context_expr() {
    // violation in the `with <here> as v:` context manager expression
    let src = "with df.collect() as rows:\n    pass";
    assert_violation(&check(d001::check, src), "D001", 1);
}

// ── Fix 7: visitor — function default args, class bases/decorators, match guards ─

#[test]
fn d001_in_function_default_arg() {
    let src = "def process(df, rows=df.collect()):\n    pass";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_class_base() {
    let src = "class Foo(df.collect()):\n    pass";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_class_decorator() {
    let src = "@some_decorator(df.collect())\nclass Foo:\n    pass";
    assert_violation(&check(d001::check, src), "D001", 1);
}

#[test]
fn d001_in_match_guard() {
    let src = "match x:\n    case _ if df.collect():\n        pass";
    assert_violation(&check(d001::check, src), "D001", 2);
}

// ── Fix 8: noqa case-insensitive ─────────────────────────────────────────────

#[test]
fn noqa_uppercase_pap_suppresses() {
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

    let src = "result = df.collect()  # NOQA: PAP: D001";
    assert!(
        check_with_noqa(d001::check, src).is_empty(),
        "uppercase NOQA: PAP should suppress"
    );

    let src2 = "result = df.collect()  # noqa: PAP: D001";
    assert!(
        check_with_noqa(d001::check, src2).is_empty(),
        "mixed-case PAP should suppress"
    );

    let src3 = "result = df.collect()  # NOQA: pap";
    assert!(
        check_with_noqa(d001::check, src3).is_empty(),
        "uppercase NOQA bare form should suppress"
    );
}

// ── Fix 1: Config hard error on malformed TOML ────────────────────────────────

#[test]
fn config_load_returns_none_for_missing_file() {
    use pyspark_antipattern::config::Config;
    let result = Config::load(std::path::Path::new("/nonexistent/pyproject.toml"));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none(), "missing file should return Ok(None)");
}

#[test]
fn config_load_returns_err_for_invalid_toml() {
    use pyspark_antipattern::config::Config;
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    writeln!(f, "[tool.pyspark-antipattern]\ndistinct_threshold = \"not_a_number\"").unwrap();
    let result = Config::load(f.path());
    assert!(result.is_err(), "malformed TOML should return Err");
}
