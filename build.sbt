name := "mps-weather-demo"

version := "0.1"

scalaVersion := "2.12.10"

val scalajVer = "2.4.2"
val sparkVer = "2.4.2"
val scope = "compile"
val configVer = "1.3.1"
lazy val commonSettings = Seq(
  version := Keys.version.toString,
  scalaVersion := Keys.scalaVersion.toString,
  libraryDependencies ++= Seq(
    // libs here
    "com.fasterxml.jackson.core" % "jackson-core" % "2.9.4",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.4",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.9.4",
    "org.apache.spark" %% "spark-core" % sparkVer % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVer % scope,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVer % scope,
    "org.apache.spark" %% "spark-streaming" % sparkVer % scope,
    "org.scalaj" %% "scalaj-http" % scalajVer % scope,
    "com.google.code.gson" % "gson" % "2.8.8",
    "com.typesafe" % "config" % configVer % scope,
  ),
  resolvers ++= Seq(
    // resolver here
    Resolver.mavenLocal
  )
)
lazy val root = project.in(file(".")).settings(
  assemblyJarName in assembly := s"mps-weather-demo-${version.value}.jar",
  commonSettings
).enablePlugins(JavaAppPackaging)

assemblyMergeStrategy in assembly := {
  case n if n.startsWith("META-INF/services/org.apache.spark.sql.sources.DataSourceRegister") => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {
    _.data.getName == "compile-0.1.0.jar"
  }
}