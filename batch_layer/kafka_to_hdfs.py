from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, to_date
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
    app_name = "KafkaSangHDFSChungKhoan"
    print(f"Đang khởi tạo Spark Session cho ứng dụng {app_name}...")

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(settings.SPARK_MASTER) \
        .getOrCreate() 

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Session đã được tạo. Master: {settings.SPARK_MASTER}")

    input_schema = schemas.get_stock_data_schema_from_kafka()
    if not input_schema:
        print("LỖI: Không thể lấy schema dữ liệu chứng khoán từ utils.schemas.")
        spark.stop()
        sys.exit(1)

    try:
        print(f"Đang đọc dữ liệu từ Kafka topic: {settings.KAFKA_TOPIC_STOCK_DATA} tại brokers: {settings.KAFKA_BROKERS}")

        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BROKERS) \
            .option("subscribe", settings.KAFKA_TOPIC_STOCK_DATA) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        print("Schema dữ liệu thô từ Kafka:")
        df.printSchema()

        if df.rdd.isEmpty():

            print(f"Không tìm thấy dữ liệu mới trong Kafka topic '{settings.KAFKA_TOPIC_STOCK_DATA}' cho lần chạy batch này.")
            spark.stop()
            return

        print("Đang phân tích dữ liệu JSON từ cột 'value' của Kafka...")

        df_with_data = df.select(
            col("key").cast("string").alias("symbol"),
            from_json(col("value").cast("string"), input_schema).alias("data")
        )

        fields_to_select_from_data = [f.name for f in input_schema.fields if f.name.lower() not in ['symbol', 'mack']]
        select_exprs_from_data = [col(f"data.{field_name}") for field_name in fields_to_select_from_data]

        parsed_df = df_with_data.select(
            col("symbol"),
            *select_exprs_from_data
        )

        print("Schema dữ liệu sau khi parse JSON (và xử lý trùng lặp cột):")
        parsed_df.printSchema()

        print("Đang chuyển đổi chuỗi ngày và tạo cột partition...")

        date_col_original = "Ngay" if "Ngay" in parsed_df.columns else "date"
        processed_df = parsed_df.withColumnRenamed(date_col_original, "original_date_str") \
                                .withColumn("event_date", to_date(col("original_date_str"), 'dd/MM/yyyy')) \
                                .withColumn("partition_date", date_format(col("event_date"), settings.BATCH_TARGET_DATE_FORMAT))

        valid_df = processed_df.filter(col("partition_date").isNotNull()) \
                               .filter(col("symbol").isNotNull())

        final_df = valid_df 

        print("Schema dữ liệu đã xử lý (có cột partition_date):")
        final_df.printSchema()
        print("Dữ liệu mẫu đã xử lý:")
        final_df.show(5, truncate=False)

        output_path = settings.HDFS_PATH_STOCK_RAW
        if not output_path:
             print("LỖI: HDFS_PATH_STOCK_RAW chưa được định nghĩa trong settings.")
             spark.stop()
             sys.exit(1)

        print(f"Đang ghi dữ liệu vào đường dẫn HDFS (phân vùng theo partition_date): {output_path}")

        final_df.write \
            .partitionBy("partition_date") \
            .mode("append") \
            .parquet(output_path)

        print(f"Đã ghi xong dữ liệu vào HDFS: {output_path}")

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý {app_name}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Đang dừng Spark session.")
        if 'spark' in locals() and spark:
            spark.stop()
            print("Spark session đã dừng.")

if __name__ == "__main__":
    main()