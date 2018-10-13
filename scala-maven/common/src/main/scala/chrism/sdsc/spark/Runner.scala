package chrism.sdsc.spark

import org.apache.spark.sql.SparkSession

trait Runner {

  final def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    run(args)(spark)

    spark.stop()
  }

  def run(args: Array[String])(implicit spark: SparkSession): Unit
}
