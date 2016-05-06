import sbt._
import sbt.Keys._


object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "edu.berkeley.cs.boom",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.6",
    //scalaVersion := "2.10.3",
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
      scalaVersion in scalaZ3 := "2.11.2",
      scalaVersion in "bloom-compiler" := "2.10.3",
      libraryDependencies ++= Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
        "org.slf4j" % "slf4j-log4j12" % "1.7.5",
        "org.scalatest" %% "scalatest" % "2.2.4",
        "org.mockito" % "mockito-core" % "1.10.19" % "test",
        "com.googlecode.kiama" %% "kiama" % "1.6.0",
        "com.github.scopt" %% "scopt" % "3.2.0",
        "org.apache.commons" % "commons-math3" % "3.2",
        "com.github.jnr" % "jnr-ffi" % "2.0.1",
        "io.argonaut" %% "argonaut" % "6.0.4",
        "org.ow2.sat4j" % "org.ow2.sat4j.core" % "2.3.5",
        "commons-io" % "commons-io" % "2.4",
        "pl.project13.scala" %% "rainbow" % "0.2" exclude("org.scalatest", "scalatest_2.11"),
        // JGraphT is used for its UnionFind data structure, which we use in
        // the type inference algorithm:
        "org.jgrapht" % "jgrapht-core" % "0.9.0",
        "nl.grons" %% "metrics-scala" % "3.2.0_a2.3",
        "com.codahale.metrics" % "metrics-json" % "3.0.2",
        "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
        "com.github.tototoshi" %% "scala-csv" % "1.0.0",
        "com.lihaoyi" %% "pprint" % "0.3.6",
        "com.github.nikita-volkov" % "sext" % "0.2.4",
        "com.github.vagm" %% "optimus" % "1.2.2"
      )
    )
  ).dependsOn(scalaZ3)

  lazy val scalaZ3 = RootProject(uri("git://github.com/JoshRosen/ScalaZ3.git#7c3d7801c7b312433f06101414aeb3a7f9f30433"))
}
