from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, date_format, expr, element_at, to_timestamp, col, split
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, TimestampType, DoubleType, DateType
import subprocess
import logging
import threading


def jobVN30Data(spark):
    print("-----------------------------------------------")
    print("Start jobVN30Data in app.py")
    print("-----------------------------------------------")

    json_schema = ArrayType(StructType([
        StructField("time", StringType(), True),
        StructField("open", IntegerType(), True),
        StructField("high", IntegerType(), True),
        StructField("low", IntegerType(), True),
        StructField("close", IntegerType(), True),
        StructField("volume", IntegerType(), True),
        StructField("ticker", StringType(), True),
        StructField("companyType", StringType(), True)
    ]))

    # Định nghĩa tham số Kafka
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "vn30",
        "startingOffsets": "latest",
        "failOnDataLoss": "false"
    }

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream.format("kafka").options(**kafka_params).load()

    print("-----------------------------------------------")
    print("Data vn30")
    print(kafka_df)
    print("-----------------------------------------------")

    # Chuyển đổi cột 'value' từ dạng binary sang chuỗi JSON
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

    stock_df = kafka_df.select(from_json(col("value"), json_schema).alias("data"))

    # Sử dụng hàm explode để biến đổi mảng thành các hàng
    stock_df = stock_df.select(explode(col("data")).alias("stock_data")).select("stock_data.*")

    # Định nghĩa đường dẫn xuất HDFS
    output_path = "hdfs://namenode:8020/user/root/kafka_data"

    # Chỉ định vị trí checkpoint
    checkpoint_location_hdfs = "hdfs://namenode:8020/user/root/checkpoints_hdfs"
  
    # Ghi dữ liệu vào HDFS dưới dạng file parquet
    hdfs_query = stock_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location_hdfs) \
        .start()
    
    hdfs_query.awaitTermination()

    # run file hadoop_to_spark.py
    subprocess.run(["python3", "hadoop_to_elastic.py"])

def jobStockRealtimeData(spark):
    print("-----------------------------------------------")
    print("Start jobStockRealtimeData  in app.py")
    print("-----------------------------------------------")
    json_schema = ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("time", StringType(), True),
        StructField("orderType", StringType(), True),
        StructField("investorType", StringType(), True),
        StructField("volume", IntegerType(), True),
        StructField("averagePrice", DoubleType(), True),
        StructField("orderCount", IntegerType(), True),
        StructField("prevPriceChange", DoubleType(), True),
        StructField("total_minutes", StringType(), True)
    ]))

    print("-----------------------------------------------")
    print("Start jobStockRealtimeData")
    print("-----------------------------------------------")

    # Định nghĩa tham số Kafka
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "stock_realtime4",
        "startingOffsets": "latest"
    }

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream.format("kafka").options(**kafka_params).load()
    print("Start jobStockRealtimeData: kafka_df")
    
    # Chuyển đổi cột 'value' từ dạng binary sang chuỗi JSON
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
    print("Start jobStockRealtimeData: kafka_df.selectExpr")

    # Chuyển đổi cột 'value' từ chuỗi JSON sang dữ liệu JSON
    data_df = kafka_df.select(from_json(col("value"), json_schema).alias("jsonData"))
    print("Start jobStockRealtimeData: data_df")

    # data_df = data_df.withColumn("jsonData.time", to_timestamp(element_at("jsonData.time", 1), "HH:mm:ss"))  # Điều chỉnh định dạng của chuỗi 'time' tương ứng

    # unique_data_df = data_df.select("jsonData.*").dropDuplicates(['total_minutes'])
    query = data_df.writeStream.outputMode("append").format("console").start()
    print("-----------------------------------------------")
    print("Start jobStockRealtimeData: query")
    print("-----------------------------------------------")

    
    try:
        data_df.writeStream .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "https://big-data.es.asia-southeast1.gcp.elastic-cloud.com") \
            .option("es.port", "9243") \
            .option("es.resource", "data_realtime_28_12") \
            .option("es.net.http.auth.user", "elastic") \
            .option("es.net.http.auth.pass", "MA6zN4P9ZenkFVx1IInSn6AK") \
            .option("es.nodes.wan.only", "true") \
            .option("checkpointLocation", "../checkpoint") \
            .outputMode("append") \
            .start()
        logging.info("Successfully upload data to Elasticsearch!")
        print("-----------------------------------------------")
        print("Successfully upload data to Elasticsearch!")
        print("-----------------------------------------------")

    except Exception as e:
        logging.error("Fail to upload data to Elasticsearch: %s", str(e))
        print("-----------------------------------------------")
        print("Fail to upload data to Elasticsearch: %s", str(e))
        print("-----------------------------------------------")


    query.awaitTermination()  

if __name__ == "__main__":
    print("-----------------------------------------------")
    print("Start app.py")
    print("-----------------------------------------------")

    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("KafkaToElasticsearch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    t1 = threading.Thread(target=jobVN30Data, args=(spark,))
    t2 = threading.Thread(target=jobStockRealtimeData, args=(spark,))
    t1.start()
    t2.start()

    t1.join()
    t2.join()
