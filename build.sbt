ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.shouneng"
ThisBuild / version := "0.1.0"

resolvers += "mapr-releases" at "https://repository.mapr.com/maven/"

val sparkVersion = "3.1.2.0-eep-800"
val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

lazy val sparkRDD = (project in file("."))
.enablePlugins(JavaAppPackaging)
.settings(
    name := "Spark RDD Example",
    libraryDependencies ++= Seq(sparkCore, sparkSql)
)