package com.practice.spark.hdfs
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object case_class {

  abstract class Event
  case class User(user_id: Int, name: String) extends Event
  case class Order(order_id: Int, user_id: Int, amount: Double) extends Event
  case class UserOrderSummary(user_id: Long, name: String, order_count: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("User Order CSV Test")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    // =========================
    // 1. CSV → Dataset
    // =========================
    val userDs: Dataset[User] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/dgkang/user.csv")
      .as[User]

    val orderDS: Dataset[Order] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/dgkang/order.csv")
      .as[Order]

    // =========================
    // 2. JOIN + 집계
    // =========================
    val resultDS: Dataset[UserOrderSummary] = userDs
      .join(orderDS, Seq("user_id"), "left")
      .groupBy("user_id", "name")
      .agg(
        sum("amount").as("total_amount"),
        count("order_id").as("order_count")
      )
      .na.fill(0, Seq("total_amount", "order_count"))
      .as[UserOrderSummary]

    // =========================
    // 3. 결과 출력
    // =========================
    resultDS.show(false)

    spark.stop()
  }
}