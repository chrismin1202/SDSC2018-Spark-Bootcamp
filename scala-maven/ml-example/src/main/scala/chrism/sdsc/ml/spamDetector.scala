package chrism.sdsc.ml

import chrism.sdsc.spark.Runner
import chrism.sdsc.util.ResourceHandle
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.matching.Regex

object SpamDetector extends Runner {

  private[ml] val DefaultNaiveBayesModelPath: String = "target/tmp/naiveBayesModel"
  private val RawCsvDataPath: String = "/chrism/sdsc/ml/spam.csv" // relative to resources directory
  private val DefaultTrainingPercentage: Double = 0.6
  private val StartsWith: Regex = "^ham|spam".r // just to filter out bad rows if any

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {

    val datasets = loadDatasets()
    val trainingDs = datasets.trainingDs
    val testDs = datasets.testDs

    // train and persist the model
    trainModel(trainingDs)
      .write
      .overwrite()
      .save(DefaultNaiveBayesModelPath)

    // Load the saved model (just to demo that trained models can be persisted and used later)
    val model = CrossValidatorModel.load(DefaultNaiveBayesModelPath)

    val predictions = model.bestModel.transform(testDs)

    println(s"area under PR: ${model.getEvaluator.evaluate(predictions)}")
  }

  /** Loads the training and test [[Dataset]] instances.
    * The split is 60/40 by default.
    *
    * @param trainingPercentage the percentage of training [[Dataset]] between (0.0, 1.0)
    * @return an instance of [[Datasets]] that contains training and test [[Dataset]]s
    */
  private[ml] def loadDatasets(trainingPercentage: Double = DefaultTrainingPercentage)
    (implicit spark: SparkSession): Datasets = {
    // Split data into training and testing
    val encodedSpamDs = encodeSpamData(loadSpamData())
    Datasets.split(encodedSpamDs, trainingPercentage)
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

  private[ml] def trainModel(trainingDs: Dataset[EncodedDataRow])
    (implicit spark: SparkSession): CrossValidatorModel = {
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

  private def loadSpamData(/* IO */)(implicit spark: SparkSession): Dataset[DataRow] = {
    import spark.implicits._

    // If the CSV is in HDFS, you should call spark.read.csv(...).
    spark.createDataset(loadCsv())
  }

  private def loadCsv(/* IO */): Seq[DataRow] =
    ResourceHandle.loadResource(RawCsvDataPath)
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

final case class Datasets(trainingDs: Dataset[EncodedDataRow], testDs: Dataset[EncodedDataRow])

object Datasets {

  private[ml] def split(ds: Dataset[EncodedDataRow], trainingPercentage: Double): Datasets = {
    require(trainingPercentage > 0.0, "The training percentage of the Dataset must be greater than 0.0!")
    require(trainingPercentage < 1.0, "The training percentage of the Dataset must be less than 1.0!")

    val testPercentage = 1.0 - trainingPercentage
    val Array(hamTrainingDs, hamTestDs) = ds
      .filter(_.label == "ham")
      .randomSplit(Array(trainingPercentage, testPercentage), 7L)

    val Array(spamTrainingDs, spamTestDs) = ds
      .filter(_.label == "spam")
      .randomSplit(Array(trainingPercentage, testPercentage), 7L)

    val trainingDs = hamTrainingDs.union(spamTrainingDs)
    val testDs = hamTestDs.union(spamTestDs)
    Datasets(trainingDs, testDs)
  }
}