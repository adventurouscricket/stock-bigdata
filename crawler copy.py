from pyspark.sql import SparkSession


def jobCrawlStockDataRealtime():
    spark = SparkSession.builder \
        .appName("Spark Elasticsearch Example") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-hadoop:7.15.2") \
        .getOrCreate()                      
    
    data_df = spark.read.format("json").load("./stock_data.json")

    try:
        # data_df.write.format("org.elasticsearch.spark.sql") \
        #     .option("es.nodes", "https://big-data.es.asia-southeast1.gcp.elastic-cloud.com") \
        #     .option("es.port", "9243") \
        #     .option("es.resource", "test_json") \
        #     .option("es.net.http.auth.user", "elastic") \
        #     .option("es.net.http.auth.pass", "MA6zN4P9ZenkFVx1IInSn6AK") \
        #     .option("es.nodes.wan.only", "true") \
        #     .option("checkpointLocation", "../checkpoint") \
        #     .save()
        #     # .outputMode("append") \
        
        spark = SparkSession.builder \
            .appName("Spark Elasticsearch Example") \
            .getOrCreate()

        data_df = spark.read.format("csv").load("test.csv")

        data_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "https://big-data.es.asia-southeast1.gcp.elastic-cloud.com") \
            .option("es.port", "9243") \
            .option("es.net.http.auth.user", "elastic") \
            .option("es.net.http.auth.pass", "MA6zN4P9ZenkFVx1IInSn6AK") \
            .option("es.resource", "test_json") \
            .mode("append") \
            .save()

        spark.stop()

        print("Successfully upload data to Elasticsearch!")
    except Exception as e:
        
        print("Fail to upload data to Elasticsearch: %s", str(e))


if __name__ == "__main__":
    jobCrawlStockDataRealtime()
