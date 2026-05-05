import sbt.Keys.libraryDependencies

ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.20"

val sparkVersion = "3.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "write_hdfs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )
