package part3dfjoins

import org.apache.spark.sql.{SaveMode, SparkSession}

object Bucketing {

  System.setProperty("hadoop.home.dir","C:\\hadoop")

  val spark = SparkSession.builder()
    .appName("Bucketing")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcasting
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val large = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
  val small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)

  val joined = large.join(small, "id")
  joined.explain()

  /*
    == Physical Plan ==
    *(5) Project [id#2L]
    +- *(5) SortMergeJoin [id#2L], [id#6L], Inner
       :- *(2) Sort [id#2L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(id#2L, 200), true, [id=#40]
       :     +- Exchange RoundRobinPartitioning(10), false, [id=#39]
       :        +- *(1) Project [(id#0L * 5) AS id#2L]
       :           +- *(1) Range (0, 1000000, step=1, splits=2)
       +- *(4) Sort [id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(id#6L, 200), true, [id=#47]
             +- Exchange RoundRobinPartitioning(3), false, [id=#46]
                +- *(3) Project [(id#4L * 3) AS id#6L]
                   +- *(3) Range (0, 10000, step=1, splits=2)
   */

  // bucketing
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_large")

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_small")

  val bucketedLarge = spark.table("bucketed_large")
  val bucketedSmall = spark.table("bucketed_small")
  val bucketedJoin = bucketedLarge.join(small, "id")
  bucketedJoin.explain()

  /*
    == Physical Plan ==
    *(5) Project [id#11L]
    +- *(5) SortMergeJoin [id#11L], [id#6L], Inner
       :- *(2) Sort [id#11L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(id#11L, 200), true, [id=#153]
       :     +- *(1) Project [id#11L]
       :        +- *(1) Filter isnotnull(id#11L)
       :           +- *(1) ColumnarToRow
       :              +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L)], Format: Parquet, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/spark-warehouse/bucketed_large], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
       +- *(4) Sort [id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(id#6L, 200), true, [id=#160]
             +- Exchange RoundRobinPartitioning(3), false, [id=#159]
                +- *(3) Project [(id#4L * 3) AS id#6L]
                   +- *(3) Range (0, 10000, step=1, splits=2)

   */

  def main(args: Array[String]): Unit = {

  }

}
