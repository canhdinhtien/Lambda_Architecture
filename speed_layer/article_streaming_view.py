from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, window, sum, lit, concat, to_timestamp, avg, count as F_count
import os
import sys
from datetime import datetime
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)


try:
    from config import settings
    from utils import schemas 
except ImportError as e:
    logging.error(f"Lỗi: Không thể nhập cài đặt (settings) hoặc schemas: {e}. Đảm bảo config/settings.py và utils/schemas.py tồn tại và có thể truy cập.")
    sys.exit(1)

def process_article_micro_batch(df, epoch_id):
    """Hàm xử lý từng micro-batch của streaming query cho dữ liệu bài viết."""
    start_time = datetime.now()

    record_count = df.count() 

    if record_count == 0:
        logging.info(f"Epoch Bài viết {epoch_id}: Không có bản ghi nào trong micro-batch này. Bỏ qua xử lý.")
        return 


    logging.info(f"--- Bắt đầu xử lý Epoch Bài viết {epoch_id} ({start_time.isoformat()}) - Số bản ghi trong micro-batch: {record_count} ---")


    required_settings_es = [
        'ES_INDEX_ARTICLE_REALTIME_VIEWS', 'ES_DOC_ID_FIELD_ARTICLE_RT',
        'ES_HOSTS', 'ES_PORT'
    ]
    missing_settings_es = False
    for setting_name in required_settings_es:
        if not hasattr(settings, setting_name):
             logging.error(f"LỖI trong Epoch {epoch_id}: Cấu hình bắt buộc '{setting_name}' chưa được định nghĩa trong settings.")
             missing_settings_es = True
    if missing_settings_es:
        return


    es_resource = settings.ES_INDEX_ARTICLE_REALTIME_VIEWS
    es_write_options = {
        "es.nodes": settings.ES_HOSTS,
        "es.port": settings.ES_PORT,
        "es.resource": es_resource,
        "es.mapping.id": settings.ES_DOC_ID_FIELD_ARTICLE_RT, 
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

        logging.info(f"Epoch Bài viết {epoch_id}: Đã ghi thành công {record_count} bản ghi vào index ES '{settings.ES_INDEX_ARTICLE_REALTIME_VIEWS}' trong {duration:.2f} giây.")

    except Exception as e:
        logging.error(f"LỖI trong Epoch {epoch_id}: Không thể ghi micro-batch bài viết vào Elasticsearch. Lỗi: {e}", exc_info=True)
    finally:
        pass

def main():
    """Hàm chính thiết lập và chạy streaming query bài viết."""
    app_name = "XuLyTocDoBaiViet"
    logging.info(f"Đang khởi tạo Spark Session cho ứng dụng {app_name} (Structured Streaming)...")

    required_settings = [
        'SPARK_MASTER', 'SPARK_PACKAGES_KAFKA', 'SPARK_PACKAGES_ES',
        'HDFS_PATH_CHECKPOINTS_SPEED_ARTICLES',
        'KAFKA_TOPIC_ARTICLES',
        'KAFKA_BROKERS', 'ES_INDEX_ARTICLE_REALTIME_VIEWS', 'ES_HOSTS',
        'ES_PORT', 'ES_DOC_ID_FIELD_ARTICLE_RT',
    ]
    missing_settings = False
    for setting_name in required_settings:
        if not hasattr(settings, setting_name):
             logging.error(f"LỖI: Cấu hình bắt buộc '{setting_name}' chưa được định nghĩa trong config/settings.py")
             missing_settings = True
    if missing_settings:
        sys.exit(1)

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(settings.SPARK_MASTER) \
        .config("spark.jars.packages", f"{settings.SPARK_PACKAGES_KAFKA},{settings.SPARK_PACKAGES_ES}") \
        .config("spark.sql.streaming.checkpointLocation", settings.HDFS_PATH_CHECKPOINTS_SPEED_ARTICLES) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logging.info(f"Spark Session đã được tạo. Master: {settings.SPARK_MASTER}")
    logging.info(f"Đang đọc dữ liệu stream từ Kafka topic: {settings.KAFKA_TOPIC_ARTICLES} tại brokers: {settings.KAFKA_BROKERS}") 
    logging.info(f"Sử dụng thư mục checkpoint: {settings.HDFS_PATH_CHECKPOINTS_SPEED_ARTICLES}") 

    input_schema = schemas.get_article_data_schema_from_kafka()
    if not input_schema:
        logging.error("LỖI: Không thể lấy schema dữ liệu bài viết từ utils.schemas.")
        spark.stop()
        sys.exit(1)

    try:
        kafka_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BROKERS) \
            .option("subscribe", settings.KAFKA_TOPIC_ARTICLES) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        logging.info("Schema dữ liệu thô từ Kafka Stream (Bài viết):")
        kafka_stream_df.printSchema()

        logging.info("Đang phân tích dữ liệu JSON (Bài viết) từ cột 'value' của Kafka...")

        df_with_data = kafka_stream_df.select(
            col("key").cast("string").alias("article_id"),
            from_json(col("value").cast("string"), input_schema).alias("data")
        )

        fields_to_select_from_data = [f.name for f in input_schema.fields if f.name.lower() != 'article_id']
        select_exprs_from_data = [col(f"data.{field_name}") for field_name in fields_to_select_from_data]

        parsed_stream_df = df_with_data.select(
            col("article_id"), 
            *select_exprs_from_data 
        )
        logging.info("Schema dữ liệu bài viết sau khi parse JSON:")
        parsed_stream_df.printSchema()

        timestamp_col_name = "processing_timestamp" 
        event_time_col = "event_time_ts" 

        if timestamp_col_name not in parsed_stream_df.columns:
            logging.error(f"LỖI: Cột timestamp '{timestamp_col_name}' không tìm thấy trong DataFrame sau khi parse.")
            spark.stop()
            sys.exit(1)

        df_with_timestamps = parsed_stream_df \
             .withColumn(event_time_col, to_timestamp(col(timestamp_col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")) 

        filtered_df = df_with_timestamps.filter(col(event_time_col).isNotNull() & col("article_id").isNotNull())

        watermarked_df = filtered_df.withWatermark(event_time_col, "1 minute")

        window_duration = "1 minute"
        slide_duration = "1 minute"

        windowed_aggregation = watermarked_df \
            .groupBy(
                window(col(event_time_col), window_duration, slide_duration)
            ) \
            .agg(
                F_count(col("article_id")).alias("article_count_window"),
                F.sum(F.length(F.col("full_content"))).alias("total_content_chars_window"),
                F.avg(F.length(F.col("full_content"))).alias("avg_content_length_window"),
            )
        logging.info("Schema sau khi aggregation theo cửa sổ:")
        windowed_aggregation.printSchema()


        realtime_article_view = windowed_aggregation.select(
                concat(
                     F.lit("article_window_"),
                     (col("window.start").cast("long") * 1000).cast("string")
                ).alias(settings.ES_DOC_ID_FIELD_ARTICLE_RT), 
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("article_count_window"),
                col("total_content_chars_window"),
                col("avg_content_length_window"),
                lit("realtime_window").alias("view_type"),
                F.current_timestamp().alias("processing_timestamp_view") 
            )

        logging.info(f"Schema cuối cùng trước khi ghi stream vào Elasticsearch: {settings.ES_INDEX_ARTICLE_REALTIME_VIEWS}")
        realtime_article_view.printSchema()


        streaming_query = realtime_article_view.writeStream \
            .outputMode("update") \
            .option("checkpointLocation", settings.HDFS_PATH_CHECKPOINTS_SPEED_ARTICLES) \
            .trigger(processingTime='1 minutes') \
            .foreachBatch(process_article_micro_batch) \
            .start() 

        logging.info(f"Truy vấn streaming bài viết '{streaming_query.name}' (ID: {streaming_query.id}) đã khởi động thành công.")
        logging.info(f"Đang ghi dữ liệu real-time view (bài viết) vào index Elasticsearch: {settings.ES_INDEX_ARTICLE_REALTIME_VIEWS}")
        logging.info("Đang chờ truy vấn streaming kết thúc (Nhấn Ctrl+C để dừng)...")

        streaming_query.awaitTermination()

    except Exception as e:
        logging.error(f"Lỗi nghiêm trọng xảy ra trong quá trình thiết lập hoặc thực thi streaming query bài viết: {e}", exc_info=True)
    finally:
        logging.info("Đang dừng Spark session...")
        if 'spark' in locals() and spark:
            spark.stop()
            logging.info("Spark session đã dừng.")

if __name__ == "__main__":
    main()