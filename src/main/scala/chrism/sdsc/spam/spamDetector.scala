package chrism.sdsc.spam

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SpamDetector {

  private val Host: String = "localhost"
  private val Port: Int = 9999

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Spam Detector").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
//          .appName("Spam Detector")
//          .master("local[*]")
          .getOrCreate()

    import spark.implicits._

    val model = SpamDetectorModel.loadModel()

    val texts = ssc.socketTextStream(Host, Port)

    texts.map(_.split(",",2))
      .map(r => DataRow(r(0), r(1)))

    /*texts*/.foreachRDD(rdd => {
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
