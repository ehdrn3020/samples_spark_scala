import sbt.Keys.{libraryDependencies, scalaVersion}

ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.20"

val spark_core = "org.apache.spark" %% "spark-core" % "3.2.0"
val spark_sql = "org.apache.spark" %% "spark-sql" % "3.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "yarn_executor_parallel",
    version := "1.0",
    libraryDependencies ++= Seq(spark_core, spark_sql)
  )
