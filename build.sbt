name := "SDSC2018-Spark-Bootcamp"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.0")
