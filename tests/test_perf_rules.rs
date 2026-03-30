mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::perf_rules::*;

// ── PERF001: .rdd.collect() instead of .toPandas() ───────────────────────────
#[test] fn perf001_fires_rdd_collect()     { assert_violation(&check(perf001::check, "rows = df.rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_fires_chain()           { assert_violation(&check(perf001::check, "rows = df.filter(col('x') > 1).rdd.collect()"), "PERF001", 1); }
#[test] fn perf001_no_collect_only()       { assert_no_violation(&check(perf001::check, "rows = df.collect()"), "PERF001"); }
#[test] fn perf001_no_topandas()           { assert_no_violation(&check(perf001::check, "pdf = df.toPandas()"), "PERF001"); }
