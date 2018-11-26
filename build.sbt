name := "Stream"

version := "0.1"

scalaVersion := "2.11.8"

// spark
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11",
  "org.apache.spark" % "spark-streaming_2.11",
  "org.apache.spark" % "spark-sql_2.11"
) .map(_ % "2.3.2")

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

// kafka to spark
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.2" excludeAll excludeJpountz
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2" excludeAll excludeJpountz

// parse
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.10.0")

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"

// unitTest
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
