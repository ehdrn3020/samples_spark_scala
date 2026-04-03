### 프로세스
```declarative
// 코드 흐름
1) df 생성 (range)
2) repartition + count
3) groupBy aggregation
4) broadcast join + aggregation

>>> result
=== Spark App Started ===
App ID: application_1773335123455_0522
=== Executor Hosts (initial) ===
server1
server2
server3
server4
server5
server6
Unique executor hosts = 6
Stage 1 count = 50000000, elapsed = 12.11 sec
Stage 2 agg1 count = 100000, elapsed = 5.56 sec
Stage 3 join count = 1000, elapsed = 6.64 sec
=== Executor Hosts (final) ===
server1
server2
server3
server4
server5
server6
Unique executor hosts = 6
+---+-----+
|pid|count|
+---+-----+
|0  |59000|
|1  |61000|
|2  |69500|
|3  |59500|
|4  |56000|
|5  |63000|
|6  |64500|
|7  |60000|
|8  |64000|
|9  |69000|
|10 |66000|
|11 |64000|
|12 |60500|
|13 |58500|
|14 |67500|
|15 |63500|
|16 |57500|
|17 |73500|
|18 |68500|
|19 |70500|
+---+-----+
only showing top 20 rows
```

### 실행
```declarative
// jar 파일 생성
sbt package

// jar 파일 실행
// 실행할 host에 jar 파일을 복사한 후 아래 명령어 실행
/rnd/spark/default/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 40 --executor-cores 2 --executor-memory 2G --driver-memory 1G --class "com.yarn_executor_parallel.main" /rnd/spark/yarn_executor_parallel_2.12-1.0.jar
```