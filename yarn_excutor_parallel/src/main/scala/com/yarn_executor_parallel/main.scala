package com.yarn_executor_parallel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

object main {
    def main(args: Array[String]): Unit = {

        //    val spark = SparkSession.builder()
        //      .appName("YARN Execute Parallel Example")
        //      .master("local[*]")
        //      .getOrCreate()

        val spark = SparkSession.builder()
                .master("yarn")
                .appName("YARN Execute Parallel Example")
                .enableHiveSupport()
                .getOrCreate()

        val sc = spark.sparkContext
        println("=== Spark App Started ===")
        println(s"App ID: ${sc.applicationId}")

        // ---------------------------
        // 1) 현재 executor host 확인
        // ---------------------------
        def printExecutorHosts(title: String): Unit = {
            val infos = sc.statusTracker.getExecutorInfos
            val hosts = infos
                    .map(_.host)
                    .filter(_ != "driver")
                    .distinct
                    .sorted

            println(s"=== $title ===")
            hosts.foreach(println)
            println(s"Unique executor hosts = ${hosts.length}")
        }

        printExecutorHosts("Executor Hosts (initial)")

        // ---------------------------
        // 2) 데이터 생성 ( 필요 시 20M ~ 100M 정도로만 조절 )
        // ---------------------------
        val n = 10000000L
        val basePartitions = 80

        val df = spark.range(0, n, 1, basePartitions)
                .withColumn("k1", col("id") % 100000)
                .withColumn("k2", col("id") % 1000)
                .withColumn("v1", rand())
                .withColumn("v2", expr("id * 3 + 7"))

        // ---------------------------
        // 3) repartition + count
        // ---------------------------
        val t1 = System.currentTimeMillis()

        val repartitioned = df.repartition(basePartitions, col("k1"))
        val cnt = repartitioned.count()

        val t2 = System.currentTimeMillis()
        println(f"Stage 1 count = $cnt, elapsed = ${(t2 - t1) / 1000.0}%.2f sec")

        // ---------------------------
        // 4) shuffle aggregation
        // ---------------------------
        val t3 = System.currentTimeMillis()

        val agg1 = repartitioned
                .groupBy("k1")
                .agg(sum("v2").as("sum_v2"))

        val agg1Cnt = agg1.count()

        val t4 = System.currentTimeMillis()
        println(f"Stage 2 agg1 count = $agg1Cnt, elapsed = ${(t4 - t3) / 1000.0}%.2f sec")

        // ---------------------------
        // 5) 작은 테이블과 join
        // broadcast 강제해서 불필요한 대형 shuffle 방지
        // ---------------------------
        val small = spark.range(0, 100000)
                .withColumnRenamed("id", "k1")

        val t5 = System.currentTimeMillis()

        val joined = repartitioned
                .join(broadcast(small), Seq("k1"), "inner")
                .groupBy("k2")
                .count()

        val joinedCnt = joined.count()

        val t6 = System.currentTimeMillis()
        println(f"Stage 3 join count = $joinedCnt, elapsed = ${(t6 - t5) / 1000.0}%.2f sec")

        // ---------------------------
        // 6) executor host 다시 확인
        // ---------------------------
        printExecutorHosts("Executor Hosts (final)")

        // ---------------------------
        // 7) 파티션 분포 일부 확인
        // ---------------------------
        val sampled = repartitioned
                .withColumn("pid", spark_partition_id())
                .groupBy("pid")
                .count()
                .orderBy("pid")

        sampled.show(20, truncate = false)

        spark.stop()

    }
}

