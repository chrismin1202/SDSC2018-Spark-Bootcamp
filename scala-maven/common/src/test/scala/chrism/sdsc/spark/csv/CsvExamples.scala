package chrism.sdsc.spark.csv

import chrism.sdsc.TestSuite
import chrism.sdsc.hadoop.TestHadoopFileSystemMixin
import chrism.sdsc.spark.TestSparkSessionMixin
import org.apache.hadoop.fs
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Column, functions}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class CsvExamples extends TestSuite with TestSparkSessionMixin with TestHadoopFileSystemMixin {

  import CsvExamples._

  private[this] lazy val csvHdfsPath: fs.Path = newHdfsPath(hdfsRoot, "csv_example", FileName)

  test("letting Spark infer schema of a csv") {
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(csvHdfsPath.toString)

    // compare the inferred schema with the pre-defined schema (CsvSchema) in the companion object
    df.printSchema()

    // The headers are used as column names.
    // +------+-------+-----+------+-----+--------------+----------------+--------+
    // |season|episode|title|rating|views|primary_writer|secondary_writer|director|
    // +------+-------+-----+------+-----+--------------+----------------+--------+
    df.show(100, truncate = false)
  }

  test("explicitly specifying the schema of a csv") {
    import spark.implicits._

    val df = spark.read
      .option("header", value = true)
      .schema(CsvSchema)
      .csv(csvHdfsPath.toString)
      .cache()

    // compare the inferred schema with the pre-defined schema (CsvSchema) in the companion object
    df.printSchema()
    assert(df.schema === CsvSchema)

    // collect all episodes written and directed by Vince Gilligan

    // SELECT df.primary_writer FROM df WHERE df.primary_writer = 'Vince Gilligan'
    // DataFrame version
    val numEpisodes1 = df
      .where(
        ((df("primary_writer") === VinceGilliganCol) || (df("secondary_writer") === VinceGilliganCol)) &&
          (df("director") === VinceGilliganCol))
      .count()
    assert(numEpisodes1 === 4L)

    // implicitly converting to Dataset
    val ds = df.as[CsvRow]
    val numEpisodes2 = ds
      // .filter is equivalent to .where but more type-safe as we use Scala class
      .filter(r =>
      (r.primary_writer == VinceGilligan || r.secondary_writer.contains(VinceGilligan)) &&
        r.director == VinceGilligan)
      .count()
    assert(numEpisodes2 === 4L)

    // SQL version
    df.createOrReplaceTempView("bb")
    spark.sql(
      s"""SELECT
         |  count(*)
         |FROM bb
         |WHERE (primary_writer = '$VinceGilligan' OR secondary_writer = '$VinceGilligan')
         |  AND director = '$VinceGilligan'""".stripMargin)
      .show(10)

    df.unpersist()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // load the sample CSV
    hdfsPutResource(CsvResourcePath, csvHdfsPath)
  }
}

private[this] object CsvExamples {

  private val FileName: String = "CsvReadExample.csv"
  private val CsvResourcePath: String = s"/chrism/sdsc/csv/$FileName"
  private val VinceGilligan: String = "Vince Gilligan"
  private val VinceGilliganCol: Column = functions.lit(VinceGilligan)

  private val CsvSchema: StructType =
    StructType(
      Seq(
        StructField("season", DataTypes.IntegerType),
        StructField("episode", DataTypes.IntegerType),
        StructField("title", DataTypes.StringType),
        StructField("rating", DataTypes.DoubleType),
        StructField("views", DataTypes.DoubleType),
        StructField("primary_writer", DataTypes.StringType),
        StructField("secondary_writer", DataTypes.StringType),
        StructField("director", DataTypes.StringType)))

  private final case class CsvRow(
      season: Int,
      episode: Int,
      title: String,
      rating: Option[Double],
      views: Option[Double],
      primary_writer: String,
      secondary_writer: Option[String],
      director: String)

}