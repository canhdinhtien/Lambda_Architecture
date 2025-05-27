from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, lit, concat, to_timestamp, avg, min as F_min, max as F_max, count as F_count
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


def process_micro_batch(df, epoch_id):
    """Hàm xử lý từng micro-batch của streaming query cho dữ liệu chứng khoán."""
    start_time = datetime.now()

    df.cache()
    record_count = df.count()

    if record_count == 0:

        df.unpersist() 
        return 


    print(f"--- Bắt đầu xử lý Epoch Chứng khoán {epoch_id} ({start_time.isoformat()}) - Số bản ghi tổng hợp: {record_count} ---")



    if not hasattr(settings, 'ES_INDEX_STOCK_REALTIME_VIEWS') or not hasattr(settings, 'ES_DOC_ID_FIELD_STOCK_RT'):

         print(f"LỖI trong Epoch {epoch_id}: Cấu hình ES_INDEX_STOCK_REALTIME_VIEWS hoặc ES_DOC_ID_FIELD_STOCK_RT chưa được định nghĩa trong settings.")
         df.unpersist()
         return


    es_resource = settings.ES_INDEX_STOCK_REALTIME_VIEWS
    es_write_options = {
        "es.nodes": settings.ES_HOSTS,
        "es.port": settings.ES_PORT,
        "es.resource": es_resource,
        "es.mapping.id": settings.ES_DOC_ID_FIELD_STOCK_RT, 
        "es.write.operation": "index", 
        "es.nodes.wan.only": "true",
        "es.batch.size.bytes": "1mb", 
        "es.batch.size.entries": "1000" 
    }


    try:

        df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_options) \
            .mode("append") \
            .save()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"Epoch {epoch_id}: Đã ghi thành công {record_count} bản ghi chứng khoán vào index ES '{settings.ES_INDEX_STOCK_REALTIME_VIEWS}' trong {duration:.2f} giây.")

    except Exception as e:

        print(f"LỖI trong Epoch {epoch_id}: Không thể ghi micro-batch chứng khoán vào Elasticsearch. Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        df.unpersist() 

def main():
    app_name = "XuLyTocDoChungKhoan" 
    print(f"Đang khởi tạo Spark Session cho ứng dụng {app_name} (Structured Streaming)...")

    required_settings = [
        'SPARK_MASTER', 'SPARK_PACKAGES_KAFKA', 'SPARK_PACKAGES_ES',
        'HDFS_PATH_CHECKPOINTS_SPEED_STOCK', 'KAFKA_TOPIC_STOCK_DATA',
        'KAFKA_BROKERS', 'ES_INDEX_STOCK_REALTIME_VIEWS', 'ES_HOSTS',
        'ES_PORT', 'ES_DOC_ID_FIELD_STOCK_RT'
    ]
    missing_settings = False
    for setting_name in required_settings:
        if not hasattr(settings, setting_name):
             print(f"LỖI: Cấu hình bắt buộc '{setting_name}' chưa được định nghĩa trong config/settings.py")
             missing_settings = True
    if missing_settings:
        sys.exit(1)

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(settings.SPARK_MASTER) \
        .config("spark.jars.packages", f"{settings.SPARK_PACKAGES_KAFKA},{settings.SPARK_PACKAGES_ES}") \
        .config("spark.sql.streaming.checkpointLocation", settings.HDFS_PATH_CHECKPOINTS_SPEED_STOCK) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Session đã được tạo. Master: {settings.SPARK_MASTER}")
    print(f"Đang đọc dữ liệu stream từ Kafka topic: {settings.KAFKA_TOPIC_STOCK_DATA} tại brokers: {settings.KAFKA_BROKERS}")
    print(f"Sử dụng thư mục checkpoint: {settings.HDFS_PATH_CHECKPOINTS_SPEED_STOCK}")

    input_schema = schemas.get_stock_data_schema_from_kafka() 
    if not input_schema:
        print("LỖI: Không thể lấy schema dữ liệu chứng khoán từ utils.schemas.")
        spark.stop()
        sys.exit(1)

    try:
        kafka_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BROKERS) \
            .option("subscribe", settings.KAFKA_TOPIC_STOCK_DATA) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        df_with_data = kafka_stream_df.select(
            col("key").cast("string").alias("symbol"), 
            from_json(col("value").cast("string"), input_schema).alias("data")
        )

        fields_to_select_from_data = [f.name for f in input_schema.fields if f.name.lower() != 'symbol' and f.name.lower() != 'mack'] 
        select_exprs_from_data = [col(f"data.{field_name}") for field_name in fields_to_select_from_data]
        parsed_stream_df = df_with_data.select(
            col("symbol"), 
            *select_exprs_from_data
        )

        timestamp_col_name = "ThoiGianXuLyProducer" if "ThoiGianXuLyProducer" in parsed_stream_df.columns else "processing_timestamp"
        event_time_df = parsed_stream_df \
            .withColumn("event_time_ts", to_timestamp(col(timestamp_col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")) \
            .withColumnRenamed(timestamp_col_name, f"{timestamp_col_name}_iso") 

        filtered_df = event_time_df.filter(col("event_time_ts").isNotNull() & col("symbol").isNotNull())

        watermarked_df = filtered_df.withWatermark("event_time_ts", "1 minute")

        price_close_col = "GiaDongCuaSo" if "GiaDongCuaSo" in watermarked_df.columns else "price_close"
        volume_col = "KLKhopLenh" if "KLKhopLenh" in watermarked_df.columns else "volume_matched"
        low_price_col = "GiaThapNhatSo" if "GiaThapNhatSo" in watermarked_df.columns else "price_low"
        high_price_col = "GiaCaoNhatSo" if "GiaCaoNhatSo" in watermarked_df.columns else "price_high"

        windowed_aggregation = watermarked_df \
            .groupBy(
                window(col("event_time_ts"), "1 minute"),
                col("symbol")
            ) \
            .agg(
                avg(price_close_col).alias("avg_price_window"),
                sum(volume_col).alias("total_volume_window"),
                F_min(low_price_col).alias("min_price_window"),
                F_max(high_price_col).alias("max_price_window"),
                F_count("*").alias("record_count_window")
            )

        realtime_stock_view = windowed_aggregation.select(
                concat(
                    col("symbol"), lit("_"),
                    (col("window.start").cast("long") * 1000).cast("string")
                ).alias(settings.ES_DOC_ID_FIELD_STOCK_RT), 
                col("symbol"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),   
                col("avg_price_window"),
                col("total_volume_window"),
                col("min_price_window"),
                col("max_price_window"),
                col("record_count_window"),
                lit("realtime").alias("view_type")       
            )

        streaming_query = realtime_stock_view.writeStream \
            .outputMode("update") \
            .option("checkpointLocation", settings.HDFS_PATH_CHECKPOINTS_SPEED_STOCK) \
            .trigger(processingTime='1 minutes') \
            .foreachBatch(process_micro_batch) \
            .start()

        print(f"Truy vấn streaming '{streaming_query.name}' (ID: {streaming_query.id}) đã khởi động thành công cho dữ liệu chứng khoán.")
        print(f"Đang ghi dữ liệu real-time view (chứng khoán) vào index Elasticsearch: {settings.ES_INDEX_STOCK_REALTIME_VIEWS}")
        print("Đang chờ truy vấn streaming kết thúc (Nhấn Ctrl+C để dừng)...")

        streaming_query.awaitTermination()

    except Exception as e:
        print(f"Lỗi nghiêm trọng xảy ra trong quá trình thiết lập hoặc thực thi streaming query: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Đang dừng Spark session...")
        if 'spark' in locals() and spark:
            spark.stop()
            print("Spark session đã dừng.")

if __name__ == "__main__":
    main()