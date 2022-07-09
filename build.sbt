ThisBuild / scalaVersion := "2.13.8"
ThisBuild / organization := "com.shouneng"
ThisBuild / version := "0.1.0"

resolvers += "mapr-releases" at "https://repository.mapr.com/maven/"

// val sparkVersion = "3.1.2.0-eep-800"
val sparkVersion = "3.3.0"
// Note the dependencies are provided
val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

lazy val sparkRDD = (project in file("."))
.enablePlugins(JavaAppPackaging)
.settings(
    name := "Spark RDD Example",
    libraryDependencies ++= Seq(sparkCore, sparkSql)
)