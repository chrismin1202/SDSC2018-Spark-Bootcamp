package chrism.sdsc.ml

import chrism.sdsc.TestSuite
import chrism.sdsc.spark.TestSparkSessionMixin
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class SpamDetectorTest extends TestSuite with TestSparkSessionMixin {

  test("train spam detection cross-validation model") {
    val datasets = SpamDetector.loadDatasets()
    val trainingDs = datasets.trainingDs
    val testDs = datasets.testDs

    // train and persist the model
    val trainedModel = SpamDetector.trainModel(trainingDs)

    // The model can be persisted and reused.
    trainedModel
      .write
      .overwrite()
      .save(SpamDetector.DefaultNaiveBayesModelPath)

    // Load the saved model (just to demo that trained models can be persisted and used later)
    val reloadedModel = CrossValidatorModel.load(SpamDetector.DefaultNaiveBayesModelPath)

    val predictions = reloadedModel.bestModel.transform(testDs)
    println(s"area under PR: ${reloadedModel.getEvaluator.evaluate(predictions)}")
  }
}