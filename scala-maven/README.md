## Multi-module Spark application example in Scala + Maven

* Scala Version: 2.11.12 (You can downgrade, but I recommend 2.11.8 or higher)
* Maven Version: 3.x
* Recommended IDE: IntelliJ

Examples
--------
* [Spark Unit Testing Setup](common/src/test/scala/chrism/sdsc): An example of how to setup unit-test base classes for Spark and Hadoop
  * [TestSuite.scala](common/src/test/scala/chrism/sdsc/TestSuite.scala): a Scalatest base class example
  * [TestSparkSessionMixin.scala](common/src/test/scala/chrism/sdsc/spark/TestSparkSessionMixin.scala): a mixin `trait` for handling `SparkSession`
  * [TestHadoopFileSystemMixin.scala](common/src/test/scala/chrism/sdsc/hadoop/TestHadoopFileSystemMixin.scala): a mixin `trait` for simulating Hadoop `FileSystem` in unit-testing environment
  * [TestSuite](common/src/test/scala/chrism/sdsc/TestSuite.scala): a Scalatest base class example
* [Spark CSV Read Examples](common/src/test/scala/chrism/sdsc/spark/csv): An example of how to setup unit-test base classes for Spark and Hadoop
* [Machine Learning](ml-example/src/main/scala/chrism/sdsc/ml): A simple `NaiveBayes`-based spam detector
  * Note that the [dataset](ml-example/src/main/resources/chrism/sdsc/ml/spam.csv) for training is in the resources folder.
* [Spark Streaming Example](streaming-example/src/main/scala/chrism/sdsc/streaming): A simple streaming job that counts the number of occurrences each word in a stream.
