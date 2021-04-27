name := "kafkahw"

version := "0.1"

scalaVersion := "2.12.13"


libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.13.0",
  "org.apache.commons" % "commons-csv" % "1.8",
  "org.apache.kafka" %% "kafka" % "2.8.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}