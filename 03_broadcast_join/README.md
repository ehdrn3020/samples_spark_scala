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
grep -E \
'spark.executor.instances|spark.executor.cores|spark.executor.memory|spark.dynamicAllocation' \
/rnd/spark/default/conf/spark-defaults.conf
```