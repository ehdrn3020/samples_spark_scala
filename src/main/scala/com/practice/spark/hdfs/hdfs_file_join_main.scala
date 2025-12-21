package com.practice.spark.hdfs

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object hdfs_file_join_main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("HDFS File Join Example")
      .master("local[*]")        // IntelliJ 로컬 실행
      .getOrCreate()

    import spark.implicits._

    // HDFS CSV 읽기
    val userDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/dgkang/user.csv")

    val orderDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/dgkang/order.csv")

    // JOIN
    val joinedDf = userDf
      .join(orderDf, Seq("user_id"), "inner")
      .select(
        $"user_id",
        $"name",
        $"order_id",
        $"amount"
      )

    // 결과 출력
    joinedDf.show(false)

    spark.stop()
  }
}
