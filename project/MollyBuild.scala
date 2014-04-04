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
        "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
        "com.googlecode.kiama" % "kiama_2.10" % "1.5.2",
        "com.github.scopt" %% "scopt" % "3.2.0",
        "org.apache.commons" % "commons-math3" % "3.2",
        "com.github.jnr" % "jnr-ffi" % "1.0.10",
        "io.argonaut" %% "argonaut" % "6.0.3",
        "org.ow2.sat4j" % "org.ow2.sat4j.core" % "2.3.5",
        "commons-io" % "commons-io" % "2.4",
        "org.jgrapht" % "jgrapht-core" % "0.9.0"
      )
    )
  )
}
