# How to run

```
Step 1: git clone 'this repository'
Step 2: cd docker-hadoop-spark-workbench
Step 3: docker-compose up -d
Step 4: create file in hdfs
Step 5: write spark code in app/app.py
Step 6: run docker-compose build --no-cache spark-submit-app
Step 7: run docker-compose up -d
Step 8: Complete
```

# Spark-submit-app

## file submit.sh

```
#!/bin/bash

export SPARK_MASTER_URL=spark://${SPARK_MASTER_NAME}:${SPARK_MASTER_PORT}
export SPARK_HOME=/spark

/wait-for-step.sh
/execute-step.sh

if [ ! -z "${SPARK_APPLICATION_JAR_LOCATION}" ]; then
    echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
    echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
    /${SPARK_HOME}/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2  \
        --class ${SPARK_APPLICATION_MAIN_CLASS} \
        --master ${SPARK_MASTER_URL} \
        --conf "spark.driver.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
        --conf "spark.executor.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
        ${SPARK_SUBMIT_ARGS} \
        ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}
else
    if [ ! -z "${SPARK_APPLICATION_PYTHON_LOCATION}" ]; then
        echo "Submit application ${SPARK_APPLICATION_PYTHON_LOCATION} to Spark master ${SPARK_MASTER_URL}"
        echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
        PYSPARK_PYTHON=python3  /spark/bin/spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2 \
            --master ${SPARK_MASTER_URL} \
            --conf "spark.driver.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
            --conf "spark.executor.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
            ${SPARK_SUBMIT_ARGS} \
            ${SPARK_APPLICATION_PYTHON_LOCATION} ${SPARK_APPLICATION_ARGS} \
    else
        echo "Not recognized application."
    fi
fi

/finish-step.sh

```

## Kafka

```
Step 1: kafka-topics.sh --create --topic thanh-test --bootstrap-server localhost:9092
Step 2: kafka-console-producer.sh --broker-list localhost:9092 --topic thanh-test
```

## Hadoop

```
hadoop fs -getmerge hdfs://namenode:8020/user/root/kafka_data/ kafka-test.json
```

### Cách chạy mới nhất (9/12/2023)

```
- Yêu cầu: 2 máy chung mạng LAN (bởi vì 1 máy yếu quá không chạy được)
- Máy 1: Chạy các container bằng docker-compose up -d (ngoại trừ elasticsearch và kibana)
- Máy 2: Chạy container elasticsearch và kibana
- Kết quả: Data sẽ được vnstock -> crawl từ my_python_app -> kafka -> spark -> hadoop & elasticsearch -> kibana (visualize)
```

### Nếu mà gặp lỗi khi chạy namenode và datanode

```
net stop winnat
docker start namenode
net start winnat

----------------------------------------------------------------
# Nếu có lỗi liên quan đến safemode namenode
hdfs dfsadmin -safemode leave

```

## How to run file hadoop_to_elastic:

spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2 ./app/hadoop_to_elastic.py
``
