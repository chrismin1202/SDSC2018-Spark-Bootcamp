package chrism.sdsc.comparison

import java.util.concurrent.ThreadLocalRandom

import chrism.sdsc.{TestSparkSessionMixin, TestSuite}
import org.apache.spark.sql.Dataset
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class PhysicalPlansTest extends TestSuite with TestSparkSessionMixin {

  private lazy val tokens: Dataset[CountableWord] = {
    import spark.implicits._

    val rand = ThreadLocalRandom.current()

    spark.createDataset(
      (1 to 10000)
        .map(_ => rand.nextInt(97, 97 + 26))
        .map(_.asInstanceOf[Char].toString)
        .map(CountableWord(_)))
  }

  test("word count using DataFrame API") {
    // The physical plan for DataFrame version is simpler than Dataset versions.
    // DataFrame API is more mature and more optimizable than Dataset API.
    PhysicalPlans.dataFrameGroupByThenSum(tokens).explain()
  }

  test("word count using Dataset API's mapGroups transformation") {
    // One key thing to note is that there is no partial aggregation.
    // Take a look at the ScalaDoc for `mapGroups` overloads in:
    // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/KeyValueGroupedDataset.scala#L183
    PhysicalPlans.datasetMapGroups(tokens).explain()
  }

  test("word count using Dataset API's reduceGroups transformation") {
    // Note the use of 'partial_reduceaggregator', which is intended for reducing the amount of data being shuffled
    // cross nodes by performing reduction in each node prior to reducing cross nodes.
    // Also note that the performance of this version is more or less the same or slower than `datasetMapGroups` version
    // because these unit tests do not run in distributed fashion.
    // When running this on a distributed file system that exploits data locality, e.g., Hadoop, this version should
    // outperform `datasetMapGroups` version.
    PhysicalPlans.datasetReduceGroups(tokens).explain()
  }
}