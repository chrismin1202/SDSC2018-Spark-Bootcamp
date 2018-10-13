package chrism.sdsc

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, SuiteMixin}

trait TestSparkSessionMixin extends SuiteMixin with BeforeAndAfterAll {
  this: TestSuite =>

  @transient
  protected final implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  override protected /* overridable */ def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }
}