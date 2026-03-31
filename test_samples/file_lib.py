from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def helper(df: DataFrame) -> DataFrame:
    """5 shuffles — export cost = 5 (no checkpoint inside)."""
    df = df.distinct()                                # shuffle 2
    return df


def medium(df: DataFrame) -> DataFrame:
    """5 shuffles with no checkpoint — PERF003 should fire inside this function."""
    df = df.groupBy("a").agg(F.sum("b"))        # shuffle 1  (groupBy+agg = 1 stage)
    df = df.repartition(200)                     # shuffle 4  (200 = Spark default, no S005)
    df = df.sortWithinPartitions("b")            # shuffle 5
    return df


def heavy(df: DataFrame) -> DataFrame:
    """10 shuffles with no checkpoint — PERF003 should fire inside this function."""
    df = df.groupBy("a").agg(F.sum("b"))        # shuffle 1  (groupBy+agg = 1 stage)
    df = df.distinct()                           # shuffle 2
    df = df.sort("c")                            # shuffle 3
    df = df.repartition(200)                     # shuffle 4  (200 = Spark default, no S005)
    df = df.sortWithinPartitions("b")            # shuffle 5
    df = df.dropDuplicates(["a"])                # shuffle 6
    df = df.orderBy("b")                         # shuffle 7
    df = df.groupBy("b").agg(F.sum("a"))        # shuffle 8
    df = df.distinct()                           # shuffle 9
    df = df.sort("a")                            # shuffle 10  ← PERF003 fires here
    return df
