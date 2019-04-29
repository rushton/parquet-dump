import sbtassembly.MergeStrategy
import sbt.Defaults

addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.14")

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.contains("services") => MergeStrategy.concat
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
    .settings(
        name := "Parquet-Dump",
        version := "1.1.1",
        organization := "com.tune",
        scalaVersion := "2.11.8",
        parallelExecution in test := false,
        scalacOptions ++= Seq(
            "-Xlint",
            "-unchecked",
            "-deprecation",
            "-Yno-adapted-args",
            "-Ywarn-dead-code",
            "-Ywarn-numeric-widen",
            "-Ywarn-unused-import"
        ),
        libraryDependencies += "org.apache.parquet" % "parquet-common" % "1.10.0",
        libraryDependencies += "org.apache.parquet" % "parquet-tools" % "1.10.0",
        libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.0",
        libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0",
        libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.13"


    )
    .configs(IntegrationTest)
    .settings(Defaults.itSettings : _*)

// Scala linter
addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.14")
