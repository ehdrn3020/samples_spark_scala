import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object Main {

  private def printSection(title: String): Unit = {
    println()
    println("=" * 80)
    println(s"  $title")
    println("=" * 80)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BroadcastJoinExample")
      .master("local[*]")
      // 실행 중 Spark가 Join 방식을 변경하지 않도록 AQE 비활성화
      .config("spark.sql.adaptive.enabled", "false")
      // 작은 테이블을 Spark가 자동으로 Broadcast하지 않도록 비활성화
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      // 예제 데이터가 작으므로 shuffle partition 수 축소
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      import spark.implicits._

      val employees = Seq(
        (1, "Kim", 10),
        (2, "Lee", 20),
        (3, "Park", 10),
        (4, "Choi", 30)
      ).toDF(
        "employee_id",
        "employee_name",
        "department_id"
      )

      val departments = Seq(
        (10, "Engineering"),
        (20, "Sales"),
        (30, "Finance")
      ).toDF(
        "department_id",
        "department_name"
      )

      printSection("입력 데이터: Employees")
      employees.show(truncate = false)

      printSection("입력 데이터: Departments")
      departments.show(truncate = false)

      /*
       * 예제 1: Broadcast Join
       *
       * 작은 departments 테이블을 각 Executor에 전달합니다.
       * employees 데이터는 department_id 기준으로 shuffle하지 않아도 됩니다.
       */
      printSection("예제 1 시작: Broadcast Join")

      val broadcastJoinResult = employees
        .join(
          broadcast(departments),
          Seq("department_id"),
          "inner"
        )
        .select(
          "employee_id",
          "employee_name",
          "department_name"
        )
        .orderBy("employee_id")

      println("[Broadcast Join 결과]")
      broadcastJoinResult.show(truncate = false)

      println("[Broadcast Join Physical Plan]")
      broadcastJoinResult.explain("formatted")

      printSection("예제 1 종료: Broadcast Join")

      /*
       * 예제 2: 일반 Join
       *
       * autoBroadcastJoinThreshold=-1이므로 자동 Broadcast가 발생하지 않습니다.
       * 일반적으로 양쪽 데이터를 department_id 기준으로 shuffle한 뒤
       * SortMergeJoin이 실행됩니다.
       */
      printSection("예제 2 시작: Shuffle Join")

      val shuffleJoinResult = employees
        .join(
          departments,
          Seq("department_id"),
          "inner"
        )
        .select(
          "employee_id",
          "employee_name",
          "department_name"
        )
        .orderBy("employee_id")

      println("[Shuffle Join 결과]")
      shuffleJoinResult.show(truncate = false)

      println("[Shuffle Join Physical Plan]")
      shuffleJoinResult.explain("formatted")

      printSection("예제 2 종료: Shuffle Join")

      printSection("모든 Join 예제 실행 완료")

    } finally {
      spark.stop()
    }
  }
}