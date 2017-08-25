name := "SparkTest"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0", 
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
)
