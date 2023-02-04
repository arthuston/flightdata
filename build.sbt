ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "FoodEnforcement"
  )


val AkkaVersion = "2.7.0"
val akkaHttpVersion = "10.4.0"
//val playVersion = "2.9.4"
val scalacticVersion = "2.8.1"
//val sparkVersion = "2.4.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
//"com.typesafe.akka" %% "akka-actor" % AkkaVersion,
//  "com.typesafe.akka" %% "ak" % AkkaVersion,
//  "com.typesafe.play" %% "play-json" % playVersion,
//  "com.typesafe.play" %% "play-ws" % "2.8.18"
//  "com.typesafe.play.lib" %% "ws" % playVersion,
//  "org.scalactic" %% "scalactic" % scalacticVersion,
//  "org.scalatest" %% "scalatest" % scalacticVersion % "test"
  //  "org.apache.spark" %% "spark-core" % sparkVersion,
  //  "org.apache.spark" %% "spark-sql" % sparkVersion
)
