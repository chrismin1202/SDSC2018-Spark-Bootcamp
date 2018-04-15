package chrism.sdsc.join

import chrism.sdsc.Runner
import org.apache.spark.sql._

object JoinExamples extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val nameTable = spark.createDataset(Seq(
      Name(1, "Rachel", "Green"),
      Name(2, "Monica", "Geller"),
      Name(3, "Phoebe", "Buffey"),
      Name(4, "Joey", "Tribbiani"),
      Name(5, "Chandler", "Bing"),
      Name(6, "Ross", "Geller")))

    val genderTable = spark.createDataset(Seq(
      Gender(1, "female"),
      Gender(2, "female"),
      Gender(3, "female"),
      Gender(4, "male"),
      Gender(5, "male"),
      Gender(6, "male")))

    val employmentTable = spark.createDataset(Seq(
      Employment(1, "Waitress"),
      Employment(2, "Chef"),
      Employment(3, "Masseuse"),
      Employment(4, "Actor"),
      Employment(5, null),
      Employment(6, "Paleontologist")))

    // Dataset INNER JOIN
    val nameGenderDs = nameTable
      .joinWith(
        genderTable,
        nameTable("id") === genderTable("id")
        /*inner join by default*/)
      .map(r => Profile(r._1.id, first = r._1.first, last = r._1.last, gender = r._2.gender))

    nameGenderDs.show(6, truncate = false)

    // Dataset LEFT OUTER JOIN
    import PimpMyJoins._

    val nonNullEmploymentDs = employmentTable
      .filter(_.jobTitle != null)

    val profileDs = nameGenderDs
      .leftOuterJoin(
      nonNullEmploymentDs,
      nameGenderDs("id") === nonNullEmploymentDs("id"))(
        // LEFT OUTER JOIN can result in right record being null
      (l, r) => if (r == null) l else l.copy(jobTitle = r.jobTitle))

    profileDs.show(6, truncate = false)

    // Try FULL OUTER JOIN
    // Note that as opposed to LEFT OUTER JOIN, either left or right record can be null in a FULL OUTER JOIN.
  }

  private object PimpMyJoins {

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

final case class Employment(id: Int, jobTitle: String)

final case class Profile(
    id: Int,
    first: String = null,
    last: String = null,
    gender: String = null,
    jobTitle: String = null)