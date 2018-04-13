package chrism.sdsc.join

import org.apache.spark.sql.{Column, SparkSession, functions}

object JoinExamples {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Spam Detector Model Creator")
      .master("local[*]")
      .getOrCreate()

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

//    val joinCol = functions.col("id")

    // Dataset inner join
    val nameGenderDs = nameTable
      .joinWith(
        genderTable,
        nameTable("id") === genderTable("id")
        /*inner join by default*/) // Dataset[(Name, Gender)]
         .map(r => Profile(r._1.id, first = r._1.first, last = r._1.last, gender = r._2.gender))
//      .map(Profile(_))

//
//      .show(6, truncate = false)
//
//
//    // Dataset inner join
    nameGenderDs
      .joinWith(
        employmentTable.flatMap(p => Seq(p, p.copy())),
        functions.col("id")/*nameGenderDs("id") === employmentTable("id")*/,
        "leftOuter")
         .map(r => Profile(r._1.id, first = r._1.first, last = r._1.last, gender = r._1.gender, jobTitle = r._2.jobTitle))
//      .map(Profile(_))
      .show(6, truncate = false)
  }

}

final case class Name(id: Int, first: String, last: String)

final case class Gender(id: Int, gender: String)

final case class Employment(id: Int, jobTitle: String)

final case class Profile(id: Int, first: String = null, last: String = null, gender: String = null, jobTitle: String = null)

object Profile {

  def apply(nameGender: (Name, Gender)): Profile =
    Profile(nameGender._1.id, first = nameGender._1.first, last = nameGender._1.last, gender = nameGender._2.gender)
}
