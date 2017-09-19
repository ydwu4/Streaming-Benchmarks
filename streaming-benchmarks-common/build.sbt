name := "StreamingBenchmarksCommon"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-java" % "1.3.2",
  "org.apache.kafka" %% "kafka" % "0.11.0.0",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "redis.clients" % "jedis" % "2.9.0",
  "net.sf.jopt-simple" % "jopt-simple" % "5.0.4",
  "com.typesafe.akka" %% "akka-actor" % "2.5.3"
)

