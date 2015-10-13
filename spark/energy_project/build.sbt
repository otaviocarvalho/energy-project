import AssemblyKeys._

name := "energy-project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.10" % "1.5.1",
    "org.apache.spark" % "spark-streaming_2.10" % "1.5.1",
    "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblySettings

mergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case "log4j.properties" => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
}
