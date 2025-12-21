import sbt.Keys.{libraryDependencies, scalaVersion}

ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.2"

val spark_core = "org.apache.spark" %% "spark-core" % "3.2.0"
val spark_sql = "org.apache.spark" %% "spark-sql" % "3.2.0"

lazy val common = (project in file("."))
  .settings(
    name := "samples_spark_scala",
    version := "1.0",
    libraryDependencies ++= Seq(spark_core, spark_sql)
  )
