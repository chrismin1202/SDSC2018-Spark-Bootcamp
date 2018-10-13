package chrism.sdsc.join

import chrism.sdsc.Runner
import org.apache.spark.sql._

object JoinExamples extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    // stubbed
  }

  def joinExample()(implicit spark: SparkSession): Dataset[NullSafeProfile] = {
    import spark.implicits._

    val nameTable = nameDs()
    val genderTable = genderDs()

    // Dataset INNER JOIN
    val nameGenderDs = nameTable
      .joinWith(
        genderTable,
        nameTable("id") === genderTable("id")
        /*inner join by default*/)
      .map(r =>
        NullSafeProfile(r._1.id, first = Option(r._1.first), last = Option(r._1.last), gender = Option(r._2.gender)))

    nameGenderDs
  }

  def leftOuterJoinExample()(implicit spark: SparkSession): Dataset[Profile] = {
    import spark.implicits._

    val employmentTable = employmentDs()

    // Dataset INNER JOIN
    val nameGenderDs = joinExample().as[Profile]

    // Dataset LEFT OUTER JOIN
    import PimpMyJoinImplicits._

    val nonNullEmploymentDs = employmentTable.filter(_.jobTitle.nonEmpty)

    val profileDs = nameGenderDs
      .leftOuterJoin(
        nonNullEmploymentDs,
        nameGenderDs("id") === nonNullEmploymentDs("id"))(
        // LEFT OUTER JOIN can result in right record being null
        (l, r) => if (r == null) l else l.copy(jobTitle = r.jobTitle.orNull))

    profileDs
  }

  def fullOuterJoinExample()(implicit spark: SparkSession): Unit = {
    // Try FULL OUTER JOIN
    // Note that as opposed to LEFT OUTER JOIN, either left or right record can be null in a FULL OUTER JOIN.
  }

  private def nameDs()(implicit spark: SparkSession): Dataset[Name] = {
    import spark.implicits._

    spark.createDataset(Seq(
      Name(1, "Rachel", "Green"),
      Name(2, "Monica", "Geller"),
      Name(3, "Phoebe", "Buffey"),
      Name(4, "Joey", "Tribbiani"),
      Name(5, "Chandler", "Bing"),
      Name(6, "Ross", "Geller")))
  }

  private def genderDs()(implicit spark: SparkSession): Dataset[Gender] = {
    import spark.implicits._

    spark.createDataset(Seq(
      Gender(1, "female"),
      Gender(2, "female"),
      Gender(3, "female"),
      Gender(4, "male"),
      Gender(5, "male"),
      Gender(6, "male")))
  }

  private def employmentDs()(implicit spark: SparkSession): Dataset[Employment] = {
    import spark.implicits._

    spark.createDataset(Seq(
      Employment(1, Some("Waitress")),
      Employment(2, Some("Chef")),
      Employment(3, Some("Masseuse")),
      Employment(4, Some("Actor")),
      // Note that you can now use Option[A] to avoid null
      Employment(5, None),
      Employment(6, Some("Paleontologist"))))
  }

  private object PimpMyJoinImplicits {

    // Just to demo so-call "pimp-my-library" pattern

    implicit final class Joiner[L](leftDs: Dataset[L]) {

      def leftOuterJoin[R, J: Encoder](rightDs: Dataset[R], condition: Column)
        (joinFunc: (L, R) => J): Dataset[J] = {
        leftDs.joinWith(rightDs, condition, "leftOuter").map(joinFunc.tupled)
      }
    }

  }

}

final case class Name(id: Int, first: String, last: String)

final case class Gender(id: Int, gender: String)

// The nullable fields above (String fields) can also be Option
final case class Employment(id: Int, jobTitle: Option[String])

final case class Profile(
    id: Int,
    first: String = null,
    last: String = null,
    gender: String = null,
    jobTitle: String = null)

// Just to show an alternative
final case class NullSafeProfile(
    id: Int,
    first: Option[String] = None,
    last: Option[String] = None,
    gender: Option[String] = None,
    jobTitle: Option[String] = None)