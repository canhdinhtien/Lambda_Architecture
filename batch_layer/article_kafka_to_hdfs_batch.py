from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, to_date, to_timestamp, expr, coalesce 
import os
import sys
from datetime import datetime

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

try:
    from config import settings
    from utils import schemas
except ImportError as e:
    print(f"Lỗi: Không thể nhập cài đặt (settings) hoặc schemas: {e}. Đảm bảo config/settings.py và utils/schemas.py tồn tại và có thể truy cập.")
    sys.exit(1)

def main():
    app_name = "KafkaSangHDFSArticlesBatch" 
    print(f"Đang khởi tạo Spark Session cho ứng dụng {app_name}...")

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(settings.SPARK_MASTER) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Session đã được tạo. Master: {settings.SPARK_MASTER}")

    input_schema = schemas.get_article_data_schema_from_kafka()
    if not input_schema:
        print("LỖI: Không thể lấy schema dữ liệu bài viết từ utils.schemas.")
        spark.stop()
        sys.exit(1)

    kafka_brokers = settings.KAFKA_BROKERS
    kafka_topic = settings.KAFKA_TOPIC_ARTICLES
    hdfs_output_path = settings.HDFS_PATH_ARTICLES_RAW

    if not hdfs_output_path:
         print("LỖI: settings.HDFS_PATH_ARTICLES_RAW chưa được định nghĩa trong settings.")
         spark.stop()
         sys.exit(1)


    try:
        print(f"Đang đọc dữ liệu từ Kafka topic (Batch mode): {kafka_topic} tại brokers: {kafka_brokers}")

        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_brokers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        print("Schema dữ liệu thô từ Kafka:")
        df.printSchema()

        if df.rdd.isEmpty():
             print(f"Không tìm thấy dữ liệu trong Kafka topic '{kafka_topic}' cho lần chạy batch này.")
             spark.stop()
             return

        print("Đang phân tích dữ liệu JSON từ cột 'value' của Kafka...")

        df_with_data = df.select(
            col("key").cast("string").alias("article_id"), 
            from_json(col("value").cast("string"), input_schema).alias("data")
        )

        fields_to_select_from_data = [f.name for f in input_schema.fields if f.name.lower() != 'article_id']
        select_exprs_from_data = [col(f"data.{field_name}") for field_name in fields_to_select_from_data]

        parsed_df = df_with_data.select(
            col("article_id"), 
            *select_exprs_from_data 
        )

        print("Schema dữ liệu sau khi parse JSON:")
        parsed_df.printSchema()

        print("Đang chuẩn hóa timestamp và tạo cột partition...")

        df_with_timestamps = parsed_df.withColumn(
             "processing_timestamp_ts", to_timestamp(col("processing_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS") 
        )

        df_with_partition_col = df_with_timestamps.withColumn(
            "partition_timestamp",
            coalesce(col("published_timestamp"), col("processing_timestamp_ts")) 
        )


        final_df = df_with_partition_col.withColumn(
             "partition_date", date_format(col("partition_timestamp"), settings.BATCH_TARGET_DATE_FORMAT)
        )

        valid_df = final_df.filter(col("partition_date").isNotNull()) \
                           .filter(col("article_id").isNotNull())

        if valid_df.rdd.isEmpty():
             print("Không có dữ liệu bài viết hợp lệ sau khi xử lý và lọc.")
             spark.stop()
             return


        print("Schema dữ liệu bài viết đã xử lý (có cột partition_date):")
        valid_df.printSchema()
        print("Dữ liệu mẫu bài viết đã xử lý:")
        valid_df.show(5, truncate=False)


        print(f"Đang ghi dữ liệu bài viết vào đường dẫn HDFS (phân vùng theo partition_date): {hdfs_output_path}")

        valid_df.write \
            .partitionBy("partition_date") \
            .mode("append") \
            .parquet(hdfs_output_path)

        print(f"Đã ghi xong dữ liệu bài viết vào HDFS: {hdfs_output_path}")

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý batch bài viết: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Đang dừng Spark session.")
        if 'spark' in locals() and spark:
            spark.stop()
            print("Spark session đã dừng.")

if __name__ == "__main__":
    main()