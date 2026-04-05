mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::s_rules::*;

// ── S001: union() without coalesce() ─────────────────────────────────────────
#[test]
fn s001_fires() {
    assert_violation(&check(s001::check, "df.union(df2)"), "S001", 1);
}
#[test]
fn s001_no_fire_coalesce() {
    assert_no_violation(&check(s001::check, "df.union(df2).coalesce(4)"), "S001");
}
#[test]
fn s001_no_set_literal() {
    assert_no_violation(&check(s001::check, "result = {1,2}.union({3,4})"), "S001");
}
#[test]
fn s001_no_set_call() {
    assert_no_violation(&check(s001::check, "result = set(a).union(set(b))"), "S001");
}

// ── S002: join() without hint ────────────────────────────────────────────────
#[test]
fn s002_fires() {
    assert_violation(&check(s002::check, "df.join(df2, 'id')"), "S002", 1);
}
#[test]
fn s002_no_fire_hint() {
    assert_no_violation(&check(s002::check, "df.hint('broadcast').join(df2, 'id')"), "S002");
}
#[test]
fn s002_no_str_join() {
    assert_no_violation(&check(s002::check, "' '.join(cols)"), "S002");
}
#[test]
fn s002_no_comma_join() {
    assert_no_violation(&check(s002::check, "','.join(str(x) for x in items)"), "S002");
}
#[test]
fn s002_no_os_path_join() {
    assert_no_violation(&check(s002::check, "os.path.join(os.getcwd(), '../..')"), "S002");
}
#[test]
fn s002_no_sys_path_join() {
    assert_no_violation(
        &check(
            s002::check,
            "sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../../../../..')))",
        ),
        "S002",
    );
}
#[test]
fn s002_no_pathlib_join() {
    assert_no_violation(&check(s002::check, "p = pathlib.Path('/tmp').joinpath('data')"), "S002");
}
#[test]
fn s002_self_df_fires() {
    assert_violation(&check(s002::check, "self.df.join(other, 'id')"), "S002", 1);
}

// ── S003: groupBy() followed by distinct() or dropDuplicates() ───────────────
#[test]
fn s003_fires_distinct() {
    assert_violation(
        &check(s003::check, "df.groupBy('x').agg({'y':'sum'}).distinct()"),
        "S003",
        1,
    );
}
#[test]
fn s003_fires_drop_duplicates() {
    assert_violation(
        &check(s003::check, "df.groupBy('x').agg({'y':'sum'}).dropDuplicates()"),
        "S003",
        1,
    );
}
#[test]
fn s003_no_false_positive() {
    assert_no_violation(&check(s003::check, "df.distinct()"), "S003");
}

// ── S004: too many distinct() — loop-aware ────────────────────────────────────
#[test]
fn s004_fires_loop() {
    // range(10): weighted count = 10 > distinct_threshold(5)
    let src = "for i in range(10):\n    df = df.distinct()\n    df = df.localCheckpoint()";
    assert_violation(&check(s004::check, src), "S004", 2);
}
#[test]
fn s004_no_fire_below_threshold() {
    let src = "df = df.distinct()";
    assert_no_violation(&check(s004::check, src), "S004");
}
#[test]
fn s004_fires_many_calls() {
    let src = "a=df.distinct()\nb=df.distinct()\nc=df.distinct()\nd=df.distinct()\ne=df.distinct()\nf=df.distinct()";
    assert_violation(&check(s004::check, src), "S004", 1);
}

// ── S005: repartition(n) where n < 200 ───────────────────────────────────────
#[test]
fn s005_fires() {
    assert_violation(&check(s005::check, "df.repartition(50)"), "S005", 1);
}
#[test]
fn s005_no_fire_200() {
    assert_no_violation(&check(s005::check, "df.repartition(200)"), "S005");
}

// ── S006: repartition(n) where n > 200 ───────────────────────────────────────
#[test]
fn s006_fires() {
    assert_violation(&check(s006::check, "df.repartition(500)"), "S006", 1);
}
#[test]
fn s006_no_fire_200() {
    assert_no_violation(&check(s006::check, "df.repartition(200)"), "S006");
}

// ── S005/S006: repartition with column arguments ──────────────────────────────
#[test]
fn s005_fires_with_column_str() {
    // repartition(n, "col") — partition count is still first arg
    assert_violation(&check(s005::check, "df.repartition(50, 'id')"), "S005", 1);
}
#[test]
fn s005_fires_with_multiple_columns() {
    assert_violation(&check(s005::check, "df.repartition(50, 'id', 'name')"), "S005", 1);
}
#[test]
fn s005_fires_keyword_numpartitions() {
    // repartition(numPartitions=50) — count passed as keyword arg
    assert_violation(&check(s005::check, "df.repartition(numPartitions=50)"), "S005", 1);
}
#[test]
fn s005_no_fire_col_only() {
    // repartition("col") — no int partition count, should not fire
    assert_no_violation(&check(s005::check, "df.repartition('id')"), "S005");
}
#[test]
fn s006_fires_with_column_str() {
    assert_violation(&check(s006::check, "df.repartition(500, 'id')"), "S006", 1);
}
#[test]
fn s006_fires_with_multiple_columns() {
    assert_violation(&check(s006::check, "df.repartition(500, 'id', 'name')"), "S006", 1);
}
#[test]
fn s006_fires_keyword_numpartitions() {
    assert_violation(&check(s006::check, "df.repartition(numPartitions=500)"), "S006", 1);
}

// ── S007: repartition(1) or coalesce(1) ──────────────────────────────────────
#[test]
fn s007_fires_repartition() {
    assert_violation(&check(s007::check, "df.repartition(1)"), "S007", 1);
}
#[test]
fn s007_fires_coalesce() {
    assert_violation(&check(s007::check, "df.coalesce(1)"), "S007", 1);
}
#[test]
fn s007_no_fire() {
    assert_no_violation(&check(s007::check, "df.coalesce(4)"), "S007");
}
#[test]
fn s007_fires_repartition_with_column() {
    // repartition(1, "col") — single partition even when column specified
    assert_violation(&check(s007::check, "df.repartition(1, 'id')"), "S007", 1);
}
#[test]
fn s007_fires_repartition_keyword() {
    assert_violation(&check(s007::check, "df.repartition(numPartitions=1)"), "S007", 1);
}
#[test]
fn s007_no_fire_repartition_keyword_safe() {
    assert_no_violation(&check(s007::check, "df.repartition(numPartitions=4)"), "S007");
}

// ── S008: too many explode() ──────────────────────────────────────────────────
#[test]
fn s008_fires() {
    let src = "a=df.select(explode(col('a')))\nb=df.select(explode(col('b')))\nc=df.select(explode(col('c')))\nd=df.select(explode(col('d')))";
    assert_violation(&check(s008::check, src), "S008", 1);
}
#[test]
fn s008_no_fire_below_threshold() {
    let src = "df.select(explode(col('tags')))";
    assert_no_violation(&check(s008::check, src), "S008");
}

// ── S009: map() on .rdd chain ────────────────────────────────────────────────
#[test]
fn s009_fires() {
    assert_violation(&check(s009::check, "df.rdd.map(lambda r: r['x'])"), "S009", 1);
}
#[test]
fn s009_fires_chained_rdd() {
    // transform before .rdd — the .rdd property is still the immediate receiver of .map()
    assert_violation(
        &check(s009::check, "df.filter(col('x') > 0).rdd.map(lambda r: r['x'])"),
        "S009",
        1,
    );
}
#[test]
fn s009_no_fire_no_rdd() {
    assert_no_violation(&check(s009::check, "df.map(lambda r: r)"), "S009");
}

// ── S010: crossJoin() ────────────────────────────────────────────────────────
#[test]
fn s010_fires() {
    assert_violation(&check(s010::check, "df.crossJoin(df2)"), "S010", 1);
}
#[test]
fn s010_no_false_positive() {
    assert_no_violation(&check(s010::check, "df.join(df2, 'id')"), "S010");
}

// ── S011: join() with no condition ───────────────────────────────────────────
#[test]
fn s011_fires() {
    assert_violation(&check(s011::check, "df.join(df2)"), "S011", 1);
}
#[test]
fn s011_no_fire_with_key() {
    assert_no_violation(&check(s011::check, "df.join(df2, 'id')"), "S011");
}
#[test]
fn s011_no_fire_str_join() {
    assert_no_violation(&check(s011::check, "' '.join(x for x in items)"), "S011");
}
#[test]
fn s011_no_fire_comma_join() {
    assert_no_violation(&check(s011::check, "','.join(cols)"), "S011");
}
#[test]
fn s011_fires_startswith_condition() {
    assert_violation(
        &check(s011::check, "df.join(df2, df['a'].startswith(df2['b']), 'left')"),
        "S011",
        1,
    );
}
#[test]
fn s011_fires_array_contains_condition() {
    assert_violation(
        &check(s011::check, "df.join(df2, array_contains(col('a'), col('b')), 'left')"),
        "S011",
        1,
    );
}
#[test]
fn s011_no_fire_col_condition() {
    assert_no_violation(&check(s011::check, "df.join(df2, col('id'), 'left')"), "S011");
}
#[test]
fn s011_no_fire_list_condition() {
    assert_no_violation(&check(s011::check, "df.join(df2, ['id', 'type'], 'left')"), "S011");
}

// ── S012: filter() on inner join ─────────────────────────────────────────────
#[test]
fn s012_fires() {
    assert_violation(
        &check(s012::check, "df.join(df2, 'id', 'inner').filter(col('age') > 18)"),
        "S012",
        1,
    );
}
#[test]
fn s012_no_fire_left_join() {
    assert_no_violation(
        &check(s012::check, "df.join(df2, 'id', 'left').filter(col('age') > 18)"),
        "S012",
    );
}
#[test]
fn s012_no_str_join_filter() {
    assert_no_violation(&check(s012::check, "' '.join(items).filter(x)"), "S012");
}

// ── S013: reduceByKey() ───────────────────────────────────────────────────────
#[test]
fn s013_fires() {
    assert_violation(
        &check(s013::check, "result = df.rdd.reduceByKey(lambda a, b: a + b)"),
        "S013",
        1,
    );
}
#[test]
fn s013_no_groupby_agg() {
    assert_no_violation(
        &check(s013::check, "result = df.groupBy('key').agg(sum('value'))"),
        "S013",
    );
}

// ── S014: distinct() or dropDuplicates() before groupBy() ────────────────────
#[test]
fn s014_fires_distinct() {
    assert_violation(
        &check(s014::check, "df.distinct().groupBy('country').agg(count('*'))"),
        "S014",
        1,
    );
}
#[test]
fn s014_fires_drop_duplicates() {
    assert_violation(
        &check(
            s014::check,
            "df.dropDuplicates(['country']).groupBy('country').agg(count('*'))",
        ),
        "S014",
        1,
    );
}
#[test]
fn s014_fires_chained() {
    assert_violation(
        &check(
            s014::check,
            "df.filter(col('active')).distinct().groupBy('country').agg(count('*'))",
        ),
        "S014",
        1,
    );
}
#[test]
fn s014_no_fire_distinct_only() {
    assert_no_violation(&check(s014::check, "df.distinct()"), "S014");
}
#[test]
fn s014_no_fire_groupby_only() {
    assert_no_violation(&check(s014::check, "df.groupBy('country').agg(count('*'))"), "S014");
}
#[test]
fn s014_no_fire_distinct_after() {
    assert_no_violation(
        &check(s014::check, "df.groupBy('country').agg(count('*')).distinct()"),
        "S014",
    );
}
