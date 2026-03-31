mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::perf_rules::*;

// ── PERF001: .rdd.collect() instead of .toPandas() ───────────────────────────
#[test] fn perf001_fires_rdd_collect()     { assert_violation(&check(perf001::check, "rows = df.rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_fires_chain()           { assert_violation(&check(perf001::check, "rows = df.filter(col('x') > 1).rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_no_collect_only()       { assert_no_violation(&check(perf001::check, "rows = df.collect()"), "PERF001"); }
#[test] fn perf001_no_topandas()           { assert_no_violation(&check(perf001::check, "pdf = df.toPandas()"), "PERF001"); }

// ── PERF003: too many shuffles without checkpoint ─────────────────────────────
#[test] fn perf003_fires_over_threshold() {
    // 10 shuffles, no checkpoint → fires
    let src = "df=df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100).join(d,'x').dropDuplicates(['a']).orderBy('b').agg(f.count('*')).distinct()";
    assert_violation(&check(perf003::check, src), "PERF003", 1);
}
#[test] fn perf003_no_violation_under_threshold() {
    // 3 shuffles, no checkpoint → silent (default threshold = 9)
    let src = "df=df.groupBy('a').agg(f.sum('b')).distinct()";
    assert_no_violation(&check(perf003::check, src), "PERF003");
}
#[test] fn perf003_checkpoint_resets_counter() {
    // 5 shuffles, checkpoint, 5 more → never exceeds 9
    let src = "df=df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100)\ndf=df.localCheckpoint()\ndf=df.join(d,'x').dropDuplicates(['a']).orderBy('b').agg(f.count('*')).distinct()";
    assert_no_violation(&check(perf003::check, src), "PERF003");
}
#[test] fn perf003_fn_cost_propagated() {
    // helper has 5 shuffles; calling it twice = 10 → fires on line 4 (second call)
    let src = "\
def helper(df):\n    return df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100)\n\
df = helper(df)\ndf = helper(df)\n";
    assert_violation(&check(perf003::check, src), "PERF003", 4);
}
#[test] fn perf003_fn_cost_no_violation_single_call() {
    // helper has 5 shuffles; calling it once = 5 → no fire (threshold=9)
    let src = "\
def helper(df):\n    return df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100)\n\
df = helper(df)\n";
    assert_no_violation(&check(perf003::check, src), "PERF003");
}
#[test] fn perf003_fn_body_checked_independently() {
    // function body itself has 10 shuffles → fires on line 2 (the body statement)
    let src = "def heavy(df):\n    df=df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100).join(d,'x').dropDuplicates(['a']).orderBy('b').agg(f.count('*')).distinct()\n    return df";
    assert_violation(&check(perf003::check, src), "PERF003", 2);
}

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
