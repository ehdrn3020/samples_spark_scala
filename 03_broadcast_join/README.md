### 실행
```declarative
cd 03_broadcast_join

// jar 파일 생성
sbt package

// 생성 확인
03_broadcast_join/target/scala-2.12/03_broadcast_join_2.12-1.0.jar

// jar 파일 hdfs에 복제

// 실행할 host에 jar 파일을 복사한 후 아래 명령어 실행
/rnd/spark/default/bin/spark-submit \
--master yarn \
--deploy-mode client \
--class "com.diff_broadcast_join" \
hdfs://namenode//user/dgkang/03_broadcast_join_2.12-1.0.0.jar

// 기본 할당 값 확인
// --num-executors 40 --executor-cores 2 --executor-memory 2G --driver-memory 1G 생략 시
grep -E \
'spark.executor.instances|spark.executor.cores|spark.executor.memory|spark.dynamicAllocation' \
/rnd/spark/default/conf/spark-defaults.conf
```

### 실행 결과
```declarative
[Broadcast Join Physical Plan]
== Physical Plan ==
* Sort (7)
+- Exchange (6)
+- * Project (5)
+- * BroadcastHashJoin Inner BuildRight (4)
:- * LocalTableScan (1)
+- BroadcastExchange (3)
+- LocalTableScan (2)


(1) LocalTableScan [codegen id : 1]
Output [3]: [employee_id#10, employee_name#11, department_id#12]
Arguments: [employee_id#10, employee_name#11, department_id#12]

(2) LocalTableScan
Output [2]: [department_id#23, department_name#24]
Arguments: [department_id#23, department_name#24]

(3) BroadcastExchange
Input [2]: [department_id#23, department_name#24]
Arguments: HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [id=#75]

(4) BroadcastHashJoin [codegen id : 1]
Left keys [1]: [department_id#12]
Right keys [1]: [department_id#23]
Join condition: None

(5) Project [codegen id : 1]
Output [3]: [employee_id#10, employee_name#11, department_name#24]
Input [5]: [employee_id#10, employee_name#11, department_id#12, department_id#23, department_name#24]

(6) Exchange
Input [3]: [employee_id#10, employee_name#11, department_name#24]
Arguments: rangepartitioning(employee_id#10 ASC NULLS FIRST, 2), ENSURE_REQUIREMENTS, [id=#85]

(7) Sort [codegen id : 2]
Input [3]: [employee_id#10, employee_name#11, department_name#24]
Arguments: [employee_id#10 ASC NULLS FIRST], true, 0



[Shuffle Join Physical Plan]
== Physical Plan ==
* Sort (10)
+- Exchange (9)
   +- * Project (8)
      +- * SortMergeJoin Inner (7)
         :- * Sort (3)
         :  +- Exchange (2)
         :     +- LocalTableScan (1)
         +- * Sort (6)
            +- Exchange (5)
               +- LocalTableScan (4)


(1) LocalTableScan
Output [3]: [employee_id#10, employee_name#11, department_id#12]
Arguments: [employee_id#10, employee_name#11, department_id#12]

(2) Exchange
Input [3]: [employee_id#10, employee_name#11, department_id#12]
Arguments: hashpartitioning(department_id#12, 2), ENSURE_REQUIREMENTS, [id=#193]

(3) Sort [codegen id : 1]
Input [3]: [employee_id#10, employee_name#11, department_id#12]
Arguments: [department_id#12 ASC NULLS FIRST], false, 0

(4) LocalTableScan
Output [2]: [department_id#23, department_name#24]
Arguments: [department_id#23, department_name#24]

(5) Exchange
Input [2]: [department_id#23, department_name#24]
Arguments: hashpartitioning(department_id#23, 2), ENSURE_REQUIREMENTS, [id=#194]

(6) Sort [codegen id : 2]
Input [2]: [department_id#23, department_name#24]
Arguments: [department_id#23 ASC NULLS FIRST], false, 0

(7) SortMergeJoin [codegen id : 3]
Left keys [1]: [department_id#12]
Right keys [1]: [department_id#23]
Join condition: None

(8) Project [codegen id : 3]
Output [3]: [employee_id#10, employee_name#11, department_name#24]
Input [5]: [employee_id#10, employee_name#11, department_id#12, department_id#23, department_name#24]

(9) Exchange
Input [3]: [employee_id#10, employee_name#11, department_name#24]
Arguments: rangepartitioning(employee_id#10 ASC NULLS FIRST, 2), ENSURE_REQUIREMENTS, [id=#213]

(10) Sort [codegen id : 4]
Input [3]: [employee_id#10, employee_name#11, department_name#24]
Arguments: [employee_id#10 ASC NULLS FIRST], true, 0
```