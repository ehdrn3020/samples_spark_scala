ThisBuild / version := "1.0.1"
ThisBuild / scalaVersion := "2.12.21"

val spark_core = "org.apache.spark" %% "spark-core" % "3.2.0"
val spark_sql = "org.apache.spark" %% "spark-sql" % "3.2.0"
val spark_hive = "org.apache.spark" %% "spark-hive" % "3.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "03_broadcast_join",
    libraryDependencies ++= Seq(spark_core, spark_sql, spark_hive)
  )
//Scala 프로젝트가 컴파일·실행할 때 필요한 외부 라이브러리 목록을 SBT에 알려주는 설정
