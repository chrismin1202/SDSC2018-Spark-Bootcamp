package chrism.sdsc.tasknotserializable

import java.io.NotSerializableException

import chrism.sdsc.{TestSparkSessionMixin, TestSuite}
import org.apache.spark.SparkException
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class TaskNotSerializableExampleTest extends TestSuite with TestSparkSessionMixin {

  test("Task not serializable example") {
    val sparkException =
      intercept[SparkException] {
        NotSerializableVersion.run(Array.empty)
      }
    assert(sparkException.getMessage.contains("Task not serializable"))
    sparkException.getCause match {
      case notSerializableException: NotSerializableException => println(notSerializableException.getMessage)
      case other => fail(s"$other is not an expected cause!")
    }
  }

  test("Serializable job") {
    // should print number1 - number10
    SerializableVersion.run(Array.empty)
  }
}