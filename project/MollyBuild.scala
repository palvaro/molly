import sbt._
import sbt.Keys._


object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "edu.berkeley.cs.boom",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.3",
    resolvers ++= Seq(
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("releases"),
      Resolver.typesafeRepo("releases")
    ),
    parallelExecution in Test := false
  )
}


object MollyBuild extends Build {

  import BuildSettings._

  lazy val root = Project(
    "molly",
    file("."),
    settings = buildSettings ++ Seq(
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
      libraryDependencies ++= Seq(
        "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
        "org.slf4j" % "slf4j-log4j12" % "1.7.5",
        "org.scalatest" % "scalatest_2.10" % "2.0",
        "com.googlecode.kiama" % "kiama_2.10" % "1.6.0",
        "com.github.scopt" %% "scopt" % "3.2.0",
        "org.apache.commons" % "commons-math3" % "3.2",
        "com.github.jnr" % "jnr-ffi" % "1.0.10",
        "io.argonaut" %% "argonaut" % "6.0.3",
        "org.ow2.sat4j" % "org.ow2.sat4j.core" % "2.3.5",
        "commons-io" % "commons-io" % "2.4",
        "pl.project13.scala" %% "rainbow" % "0.2" exclude("org.scalatest", "scalatest_2.11"),
        // JGraphT is used for its UnionFind data structure, which we use in
        // the type inference algorithm:
        "org.jgrapht" % "jgrapht-core" % "0.9.0",
        "nl.grons" %% "metrics-scala" % "3.2.0_a2.2",
        "com.codahale.metrics" % "metrics-json" % "3.0.2",
        "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
        "com.github.tototoshi" %% "scala-csv" % "1.0.0"
      )
    )
  ).dependsOn(scalaZ3)

  lazy val scalaZ3 = RootProject(uri("git://github.com/JoshRosen/ScalaZ3.git#c0f0031139178c8b955fd1348c2bcb7bde90b475"))
}
