package part3dfjoins

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object BroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  val rows = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third")
  ))

  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  // small table
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)

  // large table
  val table = spark.range(1, 10000000) // column is "id"

  // the innocent join
  val joined = table.join(lookupTable, "id")
  joined.explain()
  // joined.show() - takes an ice age

  // a smarter join
  val joinedSmart = table.join(broadcast(lookupTable), "id")
  joinedSmart.explain()
  // joinedSmart.show()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 30)

  // auto-broadcast detection
  val bigTable = spark.range(1, 100000000)
  val smallTable = spark.range(1, 10000) // size estimated by Spark - auto-broadcast
  val joinedNumber = smallTable.join(bigTable, "id")

  // deactivate auto-broadcast
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  joinedNumber.explain()

  def main(args: Array[String]): Unit = {
    Thread.sleep(100000)
  }

}
