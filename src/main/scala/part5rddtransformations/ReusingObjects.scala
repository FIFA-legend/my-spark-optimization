package part5rddtransformations

import generator.DataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReusingObjects {

  val spark = SparkSession.builder()
    .appName("Reusing JVM Objects")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    Analyze text
    Receive batches of text from data sources
    "35 // some text"

    Stats per each data source id:
    - the number of lines in total
    - total number of words in total
    - length of the longest word
    - the number of occurrences of the word "imperdiet"

    Results should be VERY FAST.
   */

  val textPath = "src/main/resources/generated/lipsum/3m.txt"
  val criticalWord = "imperdiet"

  val text = sc.textFile(textPath).map { line =>
    val tokens = line.split("//")
    (tokens(0), tokens(1))
  }

  def generateData() = {
    DataGenerator.generateText(textPath, 6000000, 3000000, 200)
  }

  //////////////////////// Version 1
  case class TextStats(nLines: Int, nWords: Int, maxWordLength: Int, occurrences: Int)

  object TextStats {
    val zero = TextStats(0, 0, 0, 0)
  }

  def collectStats() = {
    def aggregateNewRecord(textStats: TextStats, record: String): TextStats = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrences = newWords.count(_ == criticalWord)

      TextStats(
        textStats.nLines + 1,
        textStats.nLines + newWords.length,
        if (longestWord.length > textStats.maxWordLength) longestWord.length else textStats.maxWordLength,
        textStats.occurrences + newOccurrences
      )
    }

    def combineStats(stats1: TextStats, stats2: TextStats): TextStats = {
      TextStats(
        stats1.nLines + stats2.nLines,
        stats1.nWords + stats2.nWords,
        Math.max(stats1.maxWordLength, stats2.maxWordLength),
        stats1.occurrences + stats2.occurrences
      )
    }

    val aggregate: RDD[(String, TextStats)] = text.aggregateByKey(TextStats.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()
  }

  //////////////////// Version 2
  class MutableTextStats(var nLines: Int, var nWords: Int, var maxWordLength: Int, var occurrences: Int) extends Serializable

  object MutableTextStats extends Serializable {
    def zero = new MutableTextStats(0, 0, 0, 0)
  }

  def collectStats2() = {
    def aggregateNewRecord(textStats: MutableTextStats, record: String): MutableTextStats = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrences = newWords.count(_ == criticalWord)

      textStats.nLines += 1
      textStats.nLines += newWords.length
      textStats.maxWordLength = if (longestWord.length > textStats.maxWordLength) longestWord.length else textStats.maxWordLength
      textStats.occurrences += newOccurrences

      textStats
    }

    def combineStats(stats1: MutableTextStats, stats2: MutableTextStats): MutableTextStats = {
      stats1.nLines += stats2.nLines
      stats1.nWords += stats2.nWords
      stats1.maxWordLength = Math.max(stats1.maxWordLength, stats2.maxWordLength)
      stats1.occurrences += stats2.occurrences

      stats1
    }

    val aggregate: RDD[(String, MutableTextStats)] = text.aggregateByKey(MutableTextStats.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()
  }

  def main(args: Array[String]): Unit = {
    collectStats() // 4s
    collectStats2()

    Thread.sleep(1000000)
  }

}
