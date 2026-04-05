mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::d_rules::*;

// ── D001: collect() ──────────────────────────────────────────────────────────
#[test]
fn d001_fires() {
    assert_violation(&check(d001::check, "df.collect()"), "D001", 1);
}
#[test]
fn d001_fires_multiline() {
    assert_violation(&check(d001::check, "(df\n.collect())"), "D001", 2);
}
#[test]
fn d001_no_false_positive() {
    assert_no_violation(&check(d001::check, "df.filter(col('x') > 1)"), "D001");
}

// ── D002: .rdd access ────────────────────────────────────────────────────────
#[test]
fn d002_fires() {
    assert_violation(&check(d002::check, "x = df.rdd"), "D002", 1);
}
#[test]
fn d002_no_false_positive() {
    assert_no_violation(&check(d002::check, "x = df.select('a')"), "D002");
}

// ── D003: show() ─────────────────────────────────────────────────────────────
#[test]
fn d003_fires() {
    assert_violation(&check(d003::check, "df.show()"), "D003", 1);
}
#[test]
fn d003_no_false_positive() {
    assert_no_violation(&check(d003::check, "df.write.parquet('out')"), "D003");
}

// ── D004: count() ────────────────────────────────────────────────────────────
#[test]
fn d004_fires() {
    assert_violation(&check(d004::check, "n = df.count()"), "D004", 1);
}
#[test]
fn d004_no_false_positive() {
    assert_no_violation(&check(d004::check, "df.show()"), "D004");
}
#[test]
fn d004_no_str_count() {
    assert_no_violation(&check(d004::check, r#"n = "hello".count("l")"#), "D004");
}
#[test]
fn d004_no_list_count() {
    assert_no_violation(&check(d004::check, "n = [1,2,2].count(2)"), "D004");
}

// ── D005: .rdd.isEmpty() ─────────────────────────────────────────────────────
#[test]
fn d005_fires() {
    assert_violation(&check(d005::check, "x = df.rdd.isEmpty()"), "D005", 1);
}
#[test]
fn d005_no_false_positive() {
    assert_no_violation(&check(d005::check, "x = df.isEmpty()"), "D005");
}

// ── D006: count() == 0 ───────────────────────────────────────────────────────
#[test]
fn d006_fires() {
    assert_violation(&check(d006::check, "if df.count() == 0: pass"), "D006", 1);
}
#[test]
fn d006_no_false_positive() {
    assert_no_violation(&check(d006::check, "if df.isEmpty(): pass"), "D006");
}
#[test]
fn d006_no_str_count_eq() {
    assert_no_violation(&check(d006::check, r#"if "hello".count("x") == 0: pass"#), "D006");
}
#[test]
fn d006_no_list_count_eq() {
    assert_no_violation(&check(d006::check, "if [1,2].count(3) == 0: pass"), "D006");
}

// ── D007: filter().count() == 0 ──────────────────────────────────────────────
#[test]
fn d007_fires() {
    assert_violation(
        &check(d007::check, "if df.filter(col('a') > 1).count() == 0: pass"),
        "D007",
        1,
    );
}
#[test]
fn d007_no_false_positive() {
    assert_no_violation(
        &check(d007::check, "if df.filter(col('a') > 1).isEmpty(): pass"),
        "D007",
    );
}

// ── D008: display() ──────────────────────────────────────────────────────────
#[test]
fn d008_fires() {
    assert_violation(&check(d008::check, "df.display()"), "D008", 1);
}
#[test]
fn d008_no_false_positive() {
    assert_no_violation(&check(d008::check, "df.show()"), "D008");
}
