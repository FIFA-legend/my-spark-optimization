package part3dfjoins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnPruning {

  val spark = SparkSession.builder()
    .appName("Column Pruning")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars/guitars.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands/bands.json")

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.explain()

  /*
    == Physical Plan ==
    *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildLeft
    :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#34]
    :  +- *(1) Project [band#22L, guitars#23, id#24L, name#25] <-- UNNECESSARY
    :     +- *(1) Filter isnotnull(band#22L)
    :        +- FileScan json [band#22L,guitars#23,id#24L,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/guitarPlayers/gu..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
    +- *(2) Project [hometown#37, id#38L, name#39, year#40L]
       +- *(2) Filter isnotnull(id#38L)
          +- FileScan json [hometown#37,id#38L,name#39,year#40L] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/bands/bands.json], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<hometown:string,id:bigint,name:string,year:bigint>
   */

  val guitaristsWithoutBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "left_anti")
  guitaristsWithoutBandsDF.explain()
  /*
    == Physical Plan ==
    *(2) BroadcastHashJoin [band#22L], [id#38L], LeftAnti, BuildRight
    :- FileScan json [band#22L,guitars#23,id#24L,name#25] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/guitarPlayers/gu..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#63]
       +- *(1) Project [id#38L] <- COLUMN PRUNING
          +- *(1) Filter isnotnull(id#38L)
             +- FileScan json [id#38L] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/bands/bands.json], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>

   Column pruning = cut off columns that are not relevant
   = shrinks DF
   * useful for joins and groups
   */

  // project and filter pushdown
  val namesDF = guitaristsBandsDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"))
  namesDF.explain()
  /*
    == Physical Plan ==
    *(2) Project [name#25, name#39]
    +- *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildRight
       :- *(2) Project [band#22L, name#25] <- COLUMN PRUNING
       :  +- *(2) Filter isnotnull(band#22L)
       :     +- FileScan json [band#22L,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/guitarPlayers/gu..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,name:string>
       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#100]
          +- *(1) Project [id#38L, name#39]
             +- *(1) Filter isnotnull(id#38L)
                +- FileScan json [id#38L,name#39] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/bands/bands.json], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>

    Spark tends to drop columns as early as possible.
    Should be YOUR goal as well.
   */

  val rockDF = guitarPlayersDF
    .join(bandsDF, joinCondition)
    .join(guitarsDF, array_contains(guitarPlayersDF.col("guitars"), guitarsDF.col("id")))

  val essentialsDF = rockDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"), upper(guitarsDF.col("make")))
  essentialsDF.explain()
  /*
    == Physical Plan ==
    *(3) Project [name#25, name#39, upper(make#9) AS upper(make)#147] TODO the upper function is done LAST
    +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#23, id#8L)
       :- *(2) Project [guitars#23, name#25, name#39]
       :  +- *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildRight
       :     :- *(2) Project [band#22L, guitars#23, name#25] TODO <- Column pruning
       :     :  +- *(2) Filter isnotnull(band#22L)
       :     :     +- FileScan json [band#22L,guitars#23,name#25] Batched: false, DataFilters: [isnotnull(band#22L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/guitarPlayers/gu..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
       :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#154]
       :        +- *(1) Project [id#38L, name#39] TODO <- Column pruning
       :           +- *(1) Filter isnotnull(id#38L)
       :              +- FileScan json [id#38L,name#39] Batched: false, DataFilters: [isnotnull(id#38L)], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/bands/bands.json], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
       +- BroadcastExchange IdentityBroadcastMode, [id=#144] TODO <- Column pruning
          +- FileScan json [id#8L,make#9] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/D:/Work/spark-optimization-start/src/main/resources/data/guitars/guitars...., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint,make:string>
   */

  /**
    * LESSON: if you anticipate that the joined table is much larger than the table on whose column you are applying the
    * map-side operation, e.g. " * 5"m or "upper", do this operation on the small table FIRST.
    *
    * Particularly useful for outer joins
    */

  def main(args: Array[String]): Unit = {

  }

}
