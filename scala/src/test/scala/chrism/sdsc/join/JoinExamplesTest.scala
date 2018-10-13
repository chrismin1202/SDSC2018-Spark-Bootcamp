package chrism.sdsc.join

import chrism.sdsc.{TestSparkSessionMixin, TestSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class JoinExamplesTest extends TestSuite with TestSparkSessionMixin {

  test("INNER JOIN") {
    val profiles = JoinExamples.joinExample().collect()
    val expected = Seq(
      NullSafeProfile(1, first = Some("Rachel"), last = Some("Green"), gender = Some("female")),
      NullSafeProfile(2, first = Some("Monica"), last = Some("Geller"), gender = Some("female")),
      NullSafeProfile(3, first = Some("Phoebe"), last = Some("Buffey"), gender = Some("female")),
      NullSafeProfile(4, first = Some("Joey"), last = Some("Tribbiani"), gender = Some("male")),
      NullSafeProfile(5, first = Some("Chandler"), last = Some("Bing"), gender = Some("male")),
      NullSafeProfile(6, first = Some("Ross"), last = Some("Geller"), gender = Some("male")))
    profiles should contain theSameElementsAs expected
  }

  test("LEFT OUTER JOIN") {
    val profiles = JoinExamples.leftOuterJoinExample().collect()
    val expected = Seq(
      Profile(1, first = "Rachel", last = "Green", gender = "female", jobTitle = "Waitress"),
      Profile(2, first = "Monica", last = "Geller", gender = "female", jobTitle = "Chef"),
      Profile(3, first = "Phoebe", last = "Buffey", gender = "female", jobTitle = "Masseuse"),
      Profile(4, first = "Joey", last = "Tribbiani", gender = "male", jobTitle = "Actor"),
      Profile(5, first = "Chandler", last = "Bing", gender = "male"),
      Profile(6, first = "Ross", last = "Geller", gender = "male", jobTitle = "Paleontologist"))
    profiles should contain theSameElementsAs expected
  }
}