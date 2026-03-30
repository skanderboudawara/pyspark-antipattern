mod common;
use common::{assert_no_violation, assert_violation, check, check_with};
use pyspark_antipattern::{config::Config, rules::l_rules::*};

// ── L001: for loop with DF ops and no checkpoint ──────────────────────────────
#[test]
fn l001_fires_for_loop() {
    let src = "for i in range(20):\n    df = df.filter(col('x') > i)";
    assert_violation(&check(l001::check, src), "L001", 1);
}
#[test]
fn l001_no_fire_with_checkpoint() {
    let src = "for i in range(20):\n    df = df.filter(col('x') > i)\n    df = df.localCheckpoint()";
    assert_no_violation(&check(l001::check, src), "L001");
}
#[test]
fn l001_no_fire_below_threshold() {
    // range(5) <= loop_threshold(10) — should not fire
    let src = "for i in range(5):\n    df = df.filter(col('x') > i)";
    assert_no_violation(&check(l001::check, src), "L001");
}
#[test]
fn l001_fires_while_loop() {
    let src = "while True:\n    df = df.select('a')";
    assert_violation(&check(l001::check, src), "L001", 1);
}

// ── L002: while loop with DataFrame ops ──────────────────────────────────────
#[test]
fn l002_fires() {
    let src = "while True:\n    df = df.groupBy('x').agg({'y': 'sum'})";
    assert_violation(&check(l002::check, src), "L002", 1);
}
#[test]
fn l002_no_false_positive() {
    let src = "while True:\n    x = 1 + 1";
    assert_no_violation(&check(l002::check, src), "L002");
}
#[test]
fn l002_no_fire_above_threshold() {
    // set loop_threshold=100 so while (assumed 99) does not fire
    let mut cfg = Config::default();
    cfg.loop_threshold = 100;
    let src = "while True:\n    df = df.select('a')";
    assert_no_violation(&check_with(l002::check, src, &cfg), "L002");
}

// ── L003: withColumn() inside loop ───────────────────────────────────────────
#[test]
fn l003_fires_for_loop() {
    let src = "for i in range(20):\n    df = df.withColumn(f'col_{i}', col('x'))";
    assert_violation(&check(l003::check, src), "L003", 2);
}
#[test]
fn l003_fires_while_loop() {
    let src = "while True:\n    df = df.withColumn('a', col('x'))";
    assert_violation(&check(l003::check, src), "L003", 2);
}
#[test]
fn l003_no_fire_below_threshold() {
    let src = "for i in range(5):\n    df = df.withColumn(f'col_{i}', col('x'))";
    assert_no_violation(&check(l003::check, src), "L003");
}
