package chrism.sdsc.spam

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.Source

object SpamDetector {

  private val RawCsvDataPath: String = "/chrism/sdsc/spam/spam.csv" // relative to resources directory
  private val NaiveBayesModelPath: String = "target/tmp/naiveBayesModel"

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Spam Detector")
      .master("local[*]")
      .getOrCreate()

    val spamDs = encodeSpamData()

    // Split data into training and testing
    val Array(training, test) = spamDs
      .randomSplit(Array(0.7, 0.3))

    // Create a pipeline for training a model
    val naiveBayesClassifier = new NaiveBayes()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("textVec")

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setMetricName("areaUnderPR")

    val params = new ParamGridBuilder().build()

    // use cross-validator for tuning
    new CrossValidator()
      .setEstimator(naiveBayesClassifier)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(params)
      .setNumFolds(3)
      .fit(training)
      .write
      .overwrite()
      .save(NaiveBayesModelPath)

    // Load the saved model
    val model = CrossValidatorModel.load(NaiveBayesModelPath)

    model.transform(test).show(30)


    spark.stop()
  }

  private def encodeSpamData(/* IO */)(implicit spark: SparkSession): DataFrame = {
    val spamDs = loadSpamData()

    // Index the labels
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")

    // Tokenize text messages
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")

    // Vectorize the tokenized text messages
    val vectorizer = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("textVec")

    // Create a pipeline for encoding the raw CSV data
    val pipeline = new Pipeline()
      .setStages(Array(indexer, tokenizer, vectorizer))

    pipeline
      .fit(spamDs)
      .transform(spamDs)
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