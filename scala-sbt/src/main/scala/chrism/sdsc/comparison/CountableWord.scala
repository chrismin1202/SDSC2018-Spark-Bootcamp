package chrism.sdsc.comparison

final case class CountableWord(word: String, frequency: Long = 1L) {

  def +(that: CountableWord): CountableWord = {
    require(word == that.word, s"The words do not match: $word vs. ${that.word}")

    CountableWord(word, frequency + that.frequency)
  }
}