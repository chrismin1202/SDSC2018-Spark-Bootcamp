from src.spark_session import get_or_create_spark_session
from src.word_count import count_words


def test_word_count():
    """Runs word_count.

    You may need to have the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to
    where your Python3 is installed.
    If you are on Unix-like system, it's likely to be /usr/bin/python3 or /usr/bin/local/python3,
    but it depends on how you installed Python3.
    """

    blob = """
    The path of the righteous man is beset on all sides by the inequities of the selfish and the tyranny 
    of evil men. Blessed is he, who in the name of charity and good will, 
    shepherds the weak through the valley of darkness, 
    for he is truly his brother's keeper and the finder of lost children. 
    And I will strike down upon thee with great vengeance and furious anger those who would attempt to poison 
    and destroy my brothers. And you will know my name is the Lord when I lay my vengeance upon thee."""

    df = (count_words(blob, get_or_create_spark_session())
          .orderBy('frequency', ascending=False))

    df.show(20, truncate=False)

    first_row = df.first()
    assert (first_row.word == 'the')
    assert (first_row.frequency == 10)
