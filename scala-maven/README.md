## Multi-module Spark application example in Scala + Maven

* Scala Version: 2.11.12 (You can downgrade, but I recommend 2.11.8 or higher)
* Maven Version: 3.x
* Recommended IDE: IntelliJ

Examples
--------
* [Machine Learning](ml-example/src/main/scala/chrism/sdsc/ml): A simple `NaiveBayes`-based spam detector
  * Note that the [dataset](ml-example/src/main/resources/chrism/sdsc/ml/spam.csv) for training is in the resources folder.
* [Spark Streaming Example](streaming-example/src/main/scala/chrism/sdsc/streaming): A simple streaming job that counts the number of occurrences each word in a stream.
