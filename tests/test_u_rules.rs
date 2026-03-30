mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::u_rules::*;

// ── U001: @udf returning StringType ──────────────────────────────────────────
#[test]
fn u001_fires() {
    let src = "@udf(returnType=StringType())\ndef f(x): return x.upper()";
    assert_violation(&check(u001::check, src), "U001", 1);
}
#[test]
fn u001_no_fire_other_type() {
    let src = "@udf(returnType=LongType())\ndef f(x): return x * 2";
    assert_no_violation(&check(u001::check, src), "U001");
}

// ── U002: @udf returning ArrayType ───────────────────────────────────────────
#[test]
fn u002_fires() {
    let src = "@udf(returnType=ArrayType(IntegerType()))\ndef f(x): return [int(v) for v in x.split(',')]";
    assert_violation(&check(u002::check, src), "U002", 1);
}
#[test]
fn u002_no_fire_other_type() {
    let src = "@udf(returnType=LongType())\ndef f(x): return x * 2";
    assert_no_violation(&check(u002::check, src), "U002");
}

// ── U003: any @udf or @pandas_udf ────────────────────────────────────────────
#[test]
fn u003_fires_udf() {
    let src = "@udf(returnType=LongType())\ndef f(x): return x * 2";
    assert_violation(&check(u003::check, src), "U003", 1);
}
#[test]
fn u003_fires_pandas_udf() {
    let src = "@pandas_udf(LongType())\ndef f(s): return s * 2";
    assert_violation(&check(u003::check, src), "U003", 1);
}
#[test]
fn u003_no_false_positive() {
    let src = "def f(x): return x * 2";
    assert_no_violation(&check(u003::check, src), "U003");
}
