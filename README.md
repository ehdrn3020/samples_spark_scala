## Spark Scala Samples

루트 디렉터리는 여러 독립 SBT 예제 프로젝트를 모아두는 공간입니다.
각 예제는 하위 폴더에서 개별적으로 빌드하고 실행합니다.

```text
samples_spark_scala/
├── README.md
├── clean_spark_logs/
│   └── clean_spark_history.sh
├── write_hdfs/
│   ├── build.sbt
│   ├── project/build.properties
│   └── src/main/...
└── yarn_executor_parallel/
    ├── build.sbt
    ├── project/build.properties
    └── src/main/...
```

### write_hdfs

```bash
cd write_hdfs
sbt package
sbt "runMain com.practice.spark.hdfs.hdfs_file_join_main"
sbt "runMain com.practice.spark.hdfs.case_class"
```

HDFS 설정은 `write_hdfs/src/main/resources/core-site.xml`에서 관리합니다.
샘플 CSV는 `write_hdfs/files/hdfs` 아래에 있습니다.

### yarn_executor_parallel

YARN 클러스터에서 Spark executor가 여러 서버에 분산되어 병렬 실행되는지 확인하는 예제입니다.
`range`로 대량 데이터를 생성한 뒤 `repartition`, `groupBy` shuffle aggregation, `broadcast join`을 실행하면서 executor host 분포와 partition별 row 수를 출력합니다.

```bash
cd yarn_executor_parallel
sbt package
```

YARN 제출 예시는 `yarn_executor_parallel/README.md`를 참고합니다.
