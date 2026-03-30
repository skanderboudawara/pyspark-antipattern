mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::perf_rules::*;

// ── PERF001: .rdd.collect() instead of .toPandas() ───────────────────────────
#[test] fn perf001_fires_rdd_collect()     { assert_violation(&check(perf001::check, "rows = df.rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_fires_chain()           { assert_violation(&check(perf001::check, "rows = df.filter(col('x') > 1).rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_no_collect_only()       { assert_no_violation(&check(perf001::check, "rows = df.collect()"), "PERF001"); }
#[test] fn perf001_no_topandas()           { assert_no_violation(&check(perf001::check, "pdf = df.toPandas()"), "PERF001"); }

// ── PERF002: multiple getOrCreate() calls ─────────────────────────────────────
#[test] fn perf002_fires_two_calls() {
    let src = "spark = SparkSession.builder.getOrCreate()\nspark2 = SparkSession.builder.getOrCreate()";
    assert_violation(&check(perf002::check, src), "PERF002", 1);
}
#[test] fn perf002_fires_three_calls() {
    let src = "a = SparkSession.builder.getOrCreate()\nb = SparkSession.builder.getOrCreate()\nc = SparkSession.builder.getOrCreate()";
    assert_violation(&check(perf002::check, src), "PERF002", 1);
}
#[test] fn perf002_no_single_call() {
    assert_no_violation(&check(perf002::check, "spark = SparkSession.builder.getOrCreate()"), "PERF002");
}
#[test] fn perf002_no_active_session() {
    let src = "spark = SparkSession.builder.getOrCreate()\nspark2 = SparkSession.getActiveSession()";
    assert_no_violation(&check(perf002::check, src), "PERF002");
}
