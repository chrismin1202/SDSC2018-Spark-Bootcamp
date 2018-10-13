import re


def tuplize(token):
    return token, 1


def count_words(blob, spark):
    print(blob)
    tokens = map(str.lower, re.split('[^a-zA-Z0-9]+', blob))
    countable_tokens = map(tuplize, tokens)
    return (spark.createDataFrame(countable_tokens, ['word', 'frequency'])
            .groupBy('word')
            .sum('frequency')
            .withColumnRenamed('sum(frequency)', 'frequency'))
