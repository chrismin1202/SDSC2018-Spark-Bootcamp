package chrism.sdsc.comparison

import chrism.sdsc.Runner
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PhysicalPlans extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    // stubbed
    // Run the unit test
  }

  def dataFrameGroupByThenSum(ds: Dataset[_])(implicit spark: SparkSession): DataFrame = {
    val schema = ds.schema
    require(
      schema.exists(c => c.name == "word" && c.dataType == DataTypes.StringType),
      "There is no column named `word` of type STRING!")
    require(
      schema.exists(c => c.name == "frequency" && c.dataType == DataTypes.LongType),
      "There is no column named `frequency` of type BIGINT!")

    runAndMeasure {
      ds.groupBy("word")
        .sum("frequency")
    }
  }

  def datasetMapGroups(ds: Dataset[CountableWord])(implicit spark: SparkSession): Dataset[CountableWord] = {
    import spark.implicits._

    runAndMeasure {
      ds.groupByKey(_.word)
        .mapGroups((word, iterator) => CountableWord(word, iterator.map(_.frequency).sum))
    }
  }

  def datasetReduceGroups(ds: Dataset[CountableWord])(implicit spark: SparkSession): Dataset[CountableWord] = {
    import spark.implicits._

    runAndMeasure {
      ds.groupByKey(_.word)
        .reduceGroups(_ + _)
        .map(_._2)
    }
  }

  private def runAndMeasure[R](func: => R): R = {
    val start = System.currentTimeMillis()
    val ret = func
    val end = System.currentTimeMillis()
    println(s"Duration: ${end - start}ms")
    ret
  }
}