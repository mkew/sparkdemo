name := "SparkDemo"

version := "1.0"

scalaVersion := "2.10.3"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.apache.spark"       % "spark-core_2.10"              % "1.0.0",
  "org.apache.spark"       % "spark-sql_2.10"               % "1.0.0",
  "org.apache.spark"       % "spark-streaming_2.10"         % "1.0.0",
  "org.apache.spark"       % "spark-streaming-twitter_2.10" % "1.0.0"
)
    