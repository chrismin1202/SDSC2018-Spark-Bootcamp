package chrism.sdsc.ml

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SpamDetector {

  private val Host: String = "localhost"
  private val Port: Int = 9999

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Spam Detector").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

//    implicit val spark: SparkSession = SparkSession.builder()
//      .config(sparkConf)
////          .appName("Spam Detector")
////          .master("local[*]")
//          .getOrCreate()

//    import spark.implicits._

    lazy val model = SpamDetectorModel.loadModel()

    val texts = ssc.socketTextStream(Host, Port, StorageLevel.MEMORY_AND_DISK_SER)


    texts.map(_.split(",",2))
      .map(r => DataRow(r(0), r(1)))
      .foreachRDD(rdd => {
        // Get the singleton instance of SparkSession
        implicit val spark: SparkSession = SparkSessionSingleton.getOrCreate(rdd.sparkContext.getConf)

        import spark.implicits._

      val dataRowDs = rdd.toDS()
      val encodedDs = SpamDetectorModel.encodeSpamData(dataRowDs).cache()

      encodedDs.show(10)
      val collected = model.bestModel.transform(encodedDs)
        .collect()
      encodedDs.unpersist()
      collected.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

/** Lazily instantiated singleton instance of SparkSession.
  *
  * From [[https://github.com/apache/spark/blob/v2.3.0/examples/src/main/scala/org/apache/spark/examples/streaming/SqlNetworkWordCount.scala Spark Streaming Example]]
  */
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