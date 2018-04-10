package chrism.sdsc.spam

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object SpamDetector {

  private val RawCsvDataPath: String = "/chrism/sdsc/spam/spam.csv" // relative to resources directory
  private val NaiveBayesModelPath: String = "target/tmp/naiveBayesModel"

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Spam Detector")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val spamDs = loadSpamData()

    val indexer = new StringIndexer()
      .setInputCol("text")
      .setOutputCol("textIndex")
      .fit(spamDs)

    val indexed = indexer.transform(spamDs)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("textIndex"))
      .setOutputCols(Array("textVec"))
      .fit(indexed)

    val encoded = encoder
      .transform(indexed)
      .as[EncodedRow]

    // Split data into training and testing
    val Array(training, test) = encoded
      .map(_.toLabeledPoint)
      .randomSplit(Array(0.7, 0.3))

    // Train and save model
    new NaiveBayes()
      .fit(training)
      .write
      .overwrite()
      .save(NaiveBayesModelPath)

    // Load the saved model
    val model = NaiveBayesModel.load(NaiveBayesModelPath)

    model.transform(test)
      .show(20)

    spark.stop()
  }

  private def loadSpamData(/* IO */)(implicit spark: SparkSession): Dataset[DataRow] = {
    import spark.implicits._

    spark.createDataset(loadCsv())
  }

  private def loadCsv(/* IO */): Seq[DataRow] =
    Source.fromInputStream(getClass.getResourceAsStream(RawCsvDataPath), "UTF-8")
      .getLines()
      .map(_.split(",", 2))
      .map(a => DataRow(a(0), a(1)))
      .toSeq
}

private final case class DataRow(label: String, text: String)

private final case class EncodedRow(label: String, text: String, textIndex: Double, textVec: linalg.Vector) {

  def toLabeledPoint: LabeledPoint = LabeledPoint(encodeLabel, textVec)

  private def encodeLabel: Double = if (label == "spam") 0.0 else 1.0
}