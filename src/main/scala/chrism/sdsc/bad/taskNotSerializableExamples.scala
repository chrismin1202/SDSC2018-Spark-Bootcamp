package chrism.sdsc.bad

import chrism.sdsc.Runner
import org.apache.spark.sql.{Dataset, SparkSession}

object NotSerializableVersion extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // If you run this example, you will get "org.apache.spark.SparkException: Task not serializable".
    new NotSerializableJob("number")
      .prepend((1 to 10).toDS())
      .show(10, truncate = false)
  }

  // If you need to use class over object for some reason...
  private final class NotSerializableJob(prefix: String) {

    def prepend(numDs: Dataset[Int])(implicit spark: SparkSession): Dataset[String] = {
      import spark.implicits._

      // The task is not serializable because the closure (num => value + num) depends on the state of the class.
      numDs.map(num => prefix + num)
    }
  }

}

object SerializableVersion extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // The solution is rather simple.
    // You just need to isolate the executor code from the driver code.

    new SerializableJob("number")
      .prepend((1 to 10).toDS())
      .show(10, truncate = false)
  }

  // Decouple the code that is run by the executors from the driver code
  private final class SerializableJob(prefix: String) {

    def prepend(numDs: Dataset[Int])(implicit spark: SparkSession): Dataset[String] = prependHelper(numDs, prefix)

    // By passing in prefix as a parameter to the method that is run by the executors,
    // this method no longer depends on the state of the class.
    private def prependHelper(numDs: Dataset[Int], prefix: String)(implicit spark: SparkSession): Dataset[String] = {
      import spark.implicits._

      // The task is now serializable because the closure (num => value + num) no longer depends on the state of the class.
      numDs.map(num => prefix + num)
    }
  }

}