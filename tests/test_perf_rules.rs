mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::perf_rules::*;

// ── PERF001: .rdd.collect() instead of .toPandas() ───────────────────────────
#[test] fn perf001_fires_rdd_collect()     { assert_violation(&check(perf001::check, "rows = df.rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_fires_chain()           { assert_violation(&check(perf001::check, "rows = df.filter(col('x') > 1).rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_no_collect_only()       { assert_no_violation(&check(perf001::check, "rows = df.collect()"), "PERF001"); }
#[test] fn perf001_no_topandas()           { assert_no_violation(&check(perf001::check, "pdf = df.toPandas()"), "PERF001"); }

// ── PERF003: too many shuffles without checkpoint ─────────────────────────────
// groupBy().agg() counts as ONE shuffle stage (not two) — Spark executes it as
// a single Exchange node in the physical plan.
#[test] fn perf003_fires_over_threshold() {
    // groupBy+agg=1, distinct=2, sort=3, repartition=4, join=5,
    // dropDuplicates=6, orderBy=7, agg=8, distinct=9, sort=10 → fires
    let src = "df=df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100).join(d,'x').dropDuplicates(['a']).orderBy('b').agg(f.count('*')).distinct().sort('x')";
    assert_violation(&check(perf003::check, src), "PERF003", 1);
}
#[test] fn perf003_no_violation_under_threshold() {
    // groupBy+agg=1, distinct=2 → 2 shuffles, silent
    let src = "df=df.groupBy('a').agg(f.sum('b')).distinct()";
    assert_no_violation(&check(perf003::check, src), "PERF003");
}
#[test] fn perf003_checkpoint_resets_counter() {
    // before checkpoint: groupBy+agg=1, distinct=2, sort=3, repartition=4
    // after checkpoint:  join=1, dropDuplicates=2, orderBy=3, agg=4, distinct=5
    // neither segment exceeds 9
    let src = "df=df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100)\ndf=df.localCheckpoint()\ndf=df.join(d,'x').dropDuplicates(['a']).orderBy('b').agg(f.count('*')).distinct()";
    assert_no_violation(&check(perf003::check, src), "PERF003");
}
#[test] fn perf003_fn_cost_propagated() {
    // helper export cost = 4 (groupBy+agg=1, distinct=2, sort=3, repartition=4)
    // 3 calls × 4 = 12 → fires on line 5 (third call)
    let src = "\
def helper(df):\n    return df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100)\n\
df = helper(df)\ndf = helper(df)\ndf = helper(df)\n";
    assert_violation(&check(perf003::check, src), "PERF003", 5);
}
#[test] fn perf003_fn_cost_no_violation_single_call() {
    // helper export cost = 4; one call = 4 ≤ 9, no fire
    let src = "\
def helper(df):\n    return df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100)\n\
df = helper(df)\n";
    assert_no_violation(&check(perf003::check, src), "PERF003");
}
#[test] fn perf003_fn_body_checked_independently() {
    // groupBy+agg=1, distinct=2, sort=3, repartition=4, join=5,
    // dropDuplicates=6, orderBy=7, agg=8, distinct=9, sort=10 → fires on line 2
    let src = "def heavy(df):\n    df=df.groupBy('a').agg(f.sum('b')).distinct().sort('c').repartition(100).join(d,'x').dropDuplicates(['a']).orderBy('b').agg(f.count('*')).distinct().sort('x')\n    return df";
    assert_violation(&check(perf003::check, src), "PERF003", 2);
}
#[test] fn perf003_standalone_agg_counts() {
    // agg NOT preceded by groupBy still counts as a shuffle
    let src = "df=df.distinct().sort('a').repartition(100).join(d,'x').dropDuplicates(['a']).orderBy('b').agg(f.count('*')).distinct().sort('x').join(d,'y')";
    assert_violation(&check(perf003::check, src), "PERF003", 1);
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

// ── PERF004: .persist() without StorageLevel ─────────────────────────────────
#[test] fn perf004_fires_bare_persist() {
    assert_violation(&check(perf004::check, "df.persist()"), "PERF004", 1);
}
#[test] fn perf004_fires_chained() {
    assert_violation(&check(perf004::check, "df.filter(col('x') > 0).persist()"), "PERF004", 1);
}
#[test] fn perf004_no_fire_memory_only() {
    assert_no_violation(&check(perf004::check, "df.persist(StorageLevel.MEMORY_ONLY)"), "PERF004");
}
#[test] fn perf004_no_fire_memory_and_disk() {
    assert_no_violation(&check(perf004::check, "df.persist(StorageLevel.MEMORY_AND_DISK)"), "PERF004");
}
#[test] fn perf004_no_fire_disk_only() {
    assert_no_violation(&check(perf004::check, "df.persist(StorageLevel.DISK_ONLY)"), "PERF004");
}
#[test] fn perf004_no_fire_off_heap() {
    assert_no_violation(&check(perf004::check, "df.persist(StorageLevel.OFF_HEAP)"), "PERF004");
}

// ── PERF005: persisted but never unpersisted ──────────────────────────────────
#[test]
fn perf005_fires_df2_not_unpersisted() {
    // df is unpersisted, df2 is not
    let src = "df = df.persist()\ndf2 = df2.persist()\ndf.unpersist()";
    assert_violation(&check(perf005::check, src), "PERF005", 2);
}
#[test]
fn perf005_fires_alias_not_unpersisted() {
    // df2 = df.persist() — df2 never unpersisted even though df is
    let src = "df = df.persist()\ndf2 = df.persist()\ndf.unpersist()";
    assert_violation(&check(perf005::check, src), "PERF005", 2);
}
#[test]
fn perf005_fires_both_not_unpersisted() {
    let src = "df = df.persist()\ndf2 = df2.persist()";
    assert_violation(&check(perf005::check, src), "PERF005", 1);
}
#[test]
fn perf005_no_fire_both_unpersisted() {
    let src = "df = df.persist()\ndf2 = df2.persist()\ndf.unpersist()\ndf2.unpersist()";
    assert_no_violation(&check(perf005::check, src), "PERF005");
}
#[test]
fn perf005_no_fire_single_unpersisted() {
    let src = "df = df.persist()\ndf.unpersist()";
    assert_no_violation(&check(perf005::check, src), "PERF005");
}
#[test]
fn perf005_fires_inside_function() {
    let src = "def run():\n    df = df.persist()\n    df2 = df2.persist()\n    df.unpersist()";
    assert_violation(&check(perf005::check, src), "PERF005", 3);
}

// ── PERF006: checkpoint/localCheckpoint without eager argument ────────────────
#[test] fn perf006_fires_checkpoint_bare() {
    assert_violation(&check(perf006::check, "df.checkpoint()"), "PERF006", 1);
}
#[test] fn perf006_fires_local_checkpoint_bare() {
    assert_violation(&check(perf006::check, "df.localCheckpoint()"), "PERF006", 1);
}
#[test] fn perf006_no_fire_checkpoint_eager_true() {
    assert_no_violation(&check(perf006::check, "df.checkpoint(eager=True)"), "PERF006");
}
#[test] fn perf006_no_fire_checkpoint_eager_false() {
    assert_no_violation(&check(perf006::check, "df.checkpoint(eager=False)"), "PERF006");
}
#[test] fn perf006_no_fire_checkpoint_positional_true() {
    assert_no_violation(&check(perf006::check, "df.checkpoint(True)"), "PERF006");
}
#[test] fn perf006_no_fire_checkpoint_positional_false() {
    assert_no_violation(&check(perf006::check, "df.checkpoint(False)"), "PERF006");
}
#[test] fn perf006_no_fire_local_checkpoint_eager_true() {
    assert_no_violation(&check(perf006::check, "df.localCheckpoint(eager=True)"), "PERF006");
}
#[test] fn perf006_no_fire_local_checkpoint_positional_false() {
    assert_no_violation(&check(perf006::check, "df.localCheckpoint(False)"), "PERF006");
}
