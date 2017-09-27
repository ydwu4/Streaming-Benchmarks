lazy val commonSettings = Seq (
    name := "SparkBenchmarks",
    version := "1.0",
    scalaVersion := "2.11.8"
)

lazy val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0",
  "org.apache.kafka" %% "kafka" % "0.11.0.0",
  "org.json" % "json" % "20170516",
  "redis.clients" % "jedis" % "2.9.0",
  "org.sedis" %% "sedis" % "1.2.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0"
)

lazy val kafka10 = Project(id = "kafka10", base = file("kafka10"))
  .settings(
      name := "kafka10",
      commonSettings,
      libraryDependencies ++= dependencies,
      libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
  )

lazy val kafka08 = Project(id = "kafka08", base = file("kafka08"))
  .settings(
      name := "kafka08",
      commonSettings,
      libraryDependencies ++= dependencies,
      libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.0"
  )
