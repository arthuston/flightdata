ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "FoodEnforcementReport"
  )


val requestsVersion = "0.8.0"     // http requests
val playVersion = "2.9.4"         // play json
val nScalaTimeVersion = "2.32.0"  // nscala-time
val sparkVersion = "3.2.2"        // spark

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % requestsVersion,
  "com.typesafe.play" %% "play-json" % playVersion,
  "com.github.nscala-time" %% "nscala-time" % nScalaTimeVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

