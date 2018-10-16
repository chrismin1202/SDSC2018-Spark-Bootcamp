package chrism.sdsc.wordcount

import java.util.concurrent.ThreadLocalRandom

import chrism.sdsc.Runner
import chrism.sdsc.model.CountableWord
import org.apache.spark.sql.{SparkSession, functions}

object WordCount extends Runner {

  private val AsciiLowercaseA: Int = 97
  private val NumLetters: Int = 26

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val nums = 1 to 4
    spark.createDataset(nums)
      .repartition(nums.size)
      .flatMap(_ => generateRandomWords())
      .groupByKey(_.word)
      .reduceGroups(_ + _)
      .map(_._2)
      .orderBy(functions.col("frequency").desc)
      .limit(NumLetters)
      .show(NumLetters, truncate = false)
  }

  private def generateRandomWords(/* potential IO */): Seq[CountableWord] = {
    val rand = ThreadLocalRandom.current()
    (1 to 10000)
      .map(_ => {

        rand.nextInt(AsciiLowercaseA, AsciiLowercaseA + NumLetters)
      })
      .map(_.asInstanceOf[Char].toString)
      .map(CountableWord(_))
  }
}
