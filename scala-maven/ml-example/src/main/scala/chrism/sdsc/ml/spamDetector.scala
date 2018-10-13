package chrism.sdsc.ml

import chrism.sdsc.Runner
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source
import scala.util.matching.Regex

object SpamDetector extends Runner {

  private val RawCsvDataPath: String = "/chrism/sdsc/ml/spam.csv" // relative to resources directory
  private val NaiveBayesModelPath: String = "target/tmp/naiveBayesModel"

  private val StartsWith: Regex = "^ham|spam".r // just to filter out bad rows if any


  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    // Split data into training and testing
    val encodedSpamDs = encodeSpamData(loadSpamData()).cache()

    val Array(hamTrainingDs, hamTestDs) = encodedSpamDs
      .filter(_.label == "ham")
      .randomSplit(Array(0.6, 0.4), 7L)

    val Array(spamTrainingDs, spamTestDs) = encodedSpamDs
      .filter(_.label == "spam")
      .randomSplit(Array(0.6, 0.4), 7L)

    val trainingDs = hamTrainingDs.union(spamTrainingDs)
    val testDs = hamTestDs.union(spamTestDs)

    // train and persist the model
    trainModel(trainingDs)
      .write
      .overwrite()
      .save(NaiveBayesModelPath)

    // Load the saved model (just to demo that trained models can be persisted and used later)
    val model = CrossValidatorModel.load(NaiveBayesModelPath)

    val predictions = model.bestModel.transform(testDs)

    println(s"area under PR: ${model.getEvaluator.evaluate(predictions)}")
  }

  private def trainModel(trainingDs: Dataset[EncodedDataRow])(implicit spark: SparkSession): CrossValidatorModel = {
    // Create a pipeline for training a model
    val naiveBayesClassifier = new NaiveBayes()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("textVec")

    // check distribution of data spam vs. ham

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setMetricName("areaUnderPR") // should be 0.95

    val params = new ParamGridBuilder()
      .addGrid(naiveBayesClassifier.smoothing, 0.0 to 1.0 by 0.005)
      .addGrid(naiveBayesClassifier.modelType, Array("multinomial"))
      .build()

    // use cross-validator for tuning
    new CrossValidator()
      .setEstimator(naiveBayesClassifier)
      .setEstimatorParamMaps(params)
      .setEvaluator(evaluator)
      .setNumFolds(3)
      .fit(trainingDs)
  }

  private def encodeSpamData(spamDs: Dataset[DataRow])(implicit spark: SparkSession): Dataset[EncodedDataRow] = {
    import spark.implicits._

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
      .as[EncodedDataRow]
  }

  private def loadSpamData(/* IO */)(implicit spark: SparkSession): Dataset[DataRow] = {
    import spark.implicits._

    spark.createDataset(loadCsv())
  }

  private def loadCsv(/* IO */): Seq[DataRow] =
    Source.fromInputStream(getClass.getResourceAsStream(RawCsvDataPath), "UTF-8")
      .getLines()
      .filter(StartsWith.findFirstIn(_).isDefined)
      .map(_.split(",", 2))
      .map(a => DataRow(a(0), a(1)))
      .toSeq
}

final case class DataRow(label: String, text: String)

final case class EncodedDataRow(
    label: String,
    text: String,
    indexedLabel: Double,
    tokens: Seq[String],
    textVec: linalg.Vector)