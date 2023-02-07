ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "FoodEnforcement"
  )


//val AkkaVersion = "2.7.0"
//val akkaHttpVersion = "10.4.0"
val playVersion = "2.9.4"
//val scalacticVersion = "2.8.1"
////val sparkVersion = "2.4.8"

val requestsVersion = "0.8.0"
libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % requestsVersion,
  "com.typesafe.play" %% "play-json" % playVersion
)


