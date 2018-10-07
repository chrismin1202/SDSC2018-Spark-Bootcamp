package chrism.sdsc.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.matching.Regex

/** Spark Streaming Example
  *
  * This sample streaming job simply tallies the number of occurrences of each word and sorts them in descending order.
  *
  * 1. Launch this job (either in an IDE or terminal).
  * 2. While the job is running, open up port 9999 (assuming that it's not in use by another application)
  *    in another terminal so that you can start streaming data.
  *    In Unix-like OS, you can use Netcat {{{nc -lk 9999}}}.
  *    In Windows, either install Netcat or find a similar tool.
  * 3. Start streaming text data either by copying & pasting or typing and observe how the job processes the streamed data.
  *
  *
  * Refer to [[https://github.com/apache/spark/blob/v2.3.0/examples/src/main/scala/org/apache/spark/examples/streaming/SqlNetworkWordCount.scala Spark Streaming Example]]
  */
object WordCount {

  private val Host: String = "localhost"
  private val Port: Int = 9999

  private val Delimiter: Regex = "[\\s,.]+".r
  // This column instance for sorting the Dataset by WordFrequency.frequency in descending order
  private val OrderByCol: Column = functions.col("frequency").desc

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spam Detector").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    val texts = ssc.socketTextStream(Host, Port, StorageLevel.MEMORY_AND_DISK_SER)

    texts.foreachRDD(rdd => {
      // Get the singleton instance of SparkSession
      implicit val spark: SparkSession = SparkSessionSingleton.getOrCreate(rdd.sparkContext.getConf)

      import spark.implicits._

      val wordFrequencyDs = rdd.toDS()
        .flatMap(Delimiter.split)
        .map(_.toLowerCase)
        .map(WordFrequency(_))
        .groupByKey(_.word)
        .reduceGroups(_ + _)
        .map(_._2)
        .orderBy(OrderByCol)

      wordFrequencyDs.show(10, truncate = false)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

/** Lazily instantiated singleton instance of SparkSession. */
private object SparkSessionSingleton {

  @transient
  private var instance: SparkSession = _

  def getOrCreate(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

private final case class WordFrequency(word: String, frequency: Long = 1L) {

  def +(that: WordFrequency): WordFrequency = {
    require(word == that.word, s"Words do not match: $word vs. ${that.word}")
    WordFrequency(word, frequency + that.frequency)
  }
}