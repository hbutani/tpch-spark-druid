import AssemblyKeys._

// put this at the top of the file

name := "tpchdata"

version := "0.0.1"

crossScalaVersions := Seq("2.11.0", "2.10.4")

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature")

// add scala-xml dependency when needed (for Scala 2.11 and newer) in a robust way
// this mechanism supports cross-version publishing
libraryDependencies := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    // if scala 2.11+ is used, add dependency on scala-xml module
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      libraryDependencies.value ++ Seq(
        "org.scala-lang.modules" %% "scala-xml" % "1.0.1",
        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.1",
        "org.scala-lang.modules" %% "scala-swing" % "1.0.1")
    case _ =>
      //  or just libraryDependencies.value if you don't depend on scala-swing
      libraryDependencies.value :+ "org.scala-lang" % "scala-swing" % scalaVersion.value
  }
}

val sparkVersion = "1.4.0"
val hiveVersion = "1.1.0"
val hadoopVersion = "2.4.0"
val slf4jVersion = "1.7.10"
val log4jVersion = "1.2.17"
val protobufVersion = "2.4.1"
val yarnVersion = hadoopVersion
val hbaseVersion = "0.94.6"
val akkaVersion = "2.3.10"
val sprayVersion = "1.3.3"
val jettyVersion = "8.1.14.v20131031"
val jlineVersion = "2.12.1"
val jlineGroupid = "jline"
val json4sVersion = "3.2.11"
val nscalaVersion = "1.6.0"
val spklUdfsVersion = "0.0.1"
val scalatestVersion = "2.2.4"
val guavaVersion = "14.0.1"
val eclipseJettyVersion = "8.1.14.v20131031"
val sparkdateTimeVersion = "0.0.1"
val sparkCsvVersion = "1.1.0"

resolvers ++= Seq(
  "Apache" at "http://repo.maven.apache.org",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.sonatypeRepo("public"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
  "JitPack.IO" at "https://jitpack.io"
)

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % guavaVersion % "provided",
  "org.eclipse.jetty" % "jetty-server" % eclipseJettyVersion % "provided",
  "org.eclipse.jetty" % "jetty-plus" % eclipseJettyVersion % "provided",
  "org.eclipse.jetty" % "jetty-util" % eclipseJettyVersion % "provided",
  "org.eclipse.jetty" % "jetty-http" % eclipseJettyVersion % "provided",
  "org.eclipse.jetty" % "jetty-servlet" % eclipseJettyVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "com.databricks" %% "spark-csv" % sparkCsvVersion % "provided",
  "org.slf4j" % "slf4j-api" % slf4jVersion % "provided",
  //"org.slf4j" % "slf4j-simple" % slf4jVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion % "provided",
  "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % "provided",
  "log4j" % "log4j" % log4jVersion % "provided",
  "com.github.SparklineData" % "spark-datetime" % sparkdateTimeVersion % "provided",
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "com.github.scopt" %% "scopt" % "3.3.0"
)

assemblySettings

mainClass in assembly := Some("org.sparklinedata.tpch.hadoop.TpchGenFlattenedData")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)