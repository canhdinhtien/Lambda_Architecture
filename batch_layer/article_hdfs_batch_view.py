from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StringType 
import os
import sys
from datetime import datetime, timedelta
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

try:
    from config import settings
except ImportError as e:
    logging.error(f"Lỗi: Không thể nhập settings: {e}. Đảm bảo config/settings.py tồn tại và PYTHONPATH đúng.")
    sys.exit(1)

def get_target_date_article(args):
    """Xác định ngày xử lý mục tiêu từ tham số dòng lệnh hoặc mặc định là ngày hôm qua."""
    if len(args) > 1:
        try:
            target_dt = datetime.strptime(args[1], '%Y-%m-%d')
            target_date_str = target_dt.strftime('%Y-%m-%dd') 
            logging.info(f"Đang xử lý dữ liệu bài viết cho ngày partition từ tham số: {args[1]} ({target_date_str})")
            return target_date_str 
        except ValueError:
            logging.warning(f"Cảnh báo: Định dạng ngày không hợp lệ: {args[1]}. Định dạng mong muốn là YYYY-MM-DD.")
    yesterday = datetime.now() - timedelta(days=1)
    default_date_str = yesterday.strftime('%Y-%m-%dd')
    logging.info(f"Không có tham số ngày hợp lệ hoặc tham số không hợp lệ, sử dụng partition của ngày hôm qua: {default_date_str}")
    return default_date_str 


def main(target_date_partition_str):
    """Hàm chính xử lý batch view bài viết."""
    app_name = f"ArticleBatchView_{target_date_partition_str}"
    logging.info(f"Đang khởi tạo Spark Session cho ứng dụng {app_name}...")

    required_settings = [
        'SPARK_MASTER', 'SPARK_PACKAGES_ES', 'HDFS_PATH_ARTICLES_RAW',
        'ES_INDEX_ARTICLE_BATCH_VIEWS', 'ES_DOC_ID_FIELD_ARTICLE_BATCH',
        'ES_HOSTS', 'ES_PORT'
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
        .config("spark.jars.packages", settings.SPARK_PACKAGES_ES) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logging.info(f"Spark Session đã được tạo. Master: {settings.SPARK_MASTER}")

    try:
        input_path = os.path.join(settings.HDFS_PATH_ARTICLES_RAW, f"partition_date={target_date_partition_str}")
        logging.info(f"Đang đọc dữ liệu Bài viết từ đường dẫn HDFS Parquet: {input_path}")

        try:
            df = spark.read.parquet(input_path)
            logging.info(f"Đọc thành công dữ liệu Parquet cho partition ngày {target_date_partition_str}. Schema:")
            df.printSchema()
        except AnalysisException as e:
             if "Path does not exist" in str(e):
                  logging.error(f"LỖI: Đường dẫn đầu vào không tồn tại: {input_path}. Không tìm thấy dữ liệu cho partition {target_date_partition_str}. Đảm bảo tiến trình Batch Ingestion trước đó đã chạy đúng.")
                  sys.exit(1)
             else:
                  raise e
        except Exception as e:
             logging.error(f"LỖI khi đọc dữ liệu Parquet từ {input_path}: {e}", exc_info=True)
             sys.exit(1)

        if df.rdd.isEmpty():
            logging.warning(f"DataFrame rỗng sau khi đọc từ {input_path}. Không có dữ liệu để xử lý batch view cho ngày này.")
            spark.stop()
            return


        logging.info("Đang tính toán bản tóm tắt bài viết hàng ngày (batch view)...")

        daily_article_summary = df.agg(
            F.count(F.col("article_id")).alias("total_article_count"),
            F.sum(F.length(F.col("full_content"))).alias("total_content_chars"), 
            F.avg(F.length(F.col("full_content"))).alias("avg_content_length"), 
        )

        batch_view_final = daily_article_summary \
                                .withColumn("partition_date", F.lit(target_date_partition_str)) \
                                .withColumn("view_type", F.lit("batch_daily")) \
                                .withColumn("processing_timestamp", F.current_timestamp()) 

        doc_id_col = settings.ES_DOC_ID_FIELD_ARTICLE_BATCH

        logging.info("Kết quả Article Batch View đã tính toán:")
        batch_view_final.show(10, truncate=False)
        batch_view_final.printSchema()


        es_resource = settings.ES_INDEX_ARTICLE_BATCH_VIEWS
        es_write_options = {
            "es.nodes": settings.ES_HOSTS,
            "es.port": settings.ES_PORT,
            "es.resource": es_resource,
            "es.mapping.id": settings.ES_DOC_ID_FIELD_ARTICLE_BATCH, 
            "es.write.operation": "index", 
            "es.nodes.wan.only": "true" 
        }
        logging.info(f"Đang ghi article batch view vào Elasticsearch: {es_resource} trên nodes {settings.ES_HOSTS}:{settings.ES_PORT}")

        batch_view_final.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_options) \
            .mode("append") \
            .save()

        logging.info(f"Đã ghi thành công article batch view cho partition ngày {target_date_partition_str} vào Elasticsearch.")

    except Exception as e:
        logging.error(f"Đã xảy ra lỗi trong quá trình xử lý batch bài viết cho partition {target_date_partition_str}: {e}", exc_info=True)
    finally:
        logging.info("Đang dừng Spark session.")
        if 'spark' in locals() and spark:
             spark.stop()
             logging.info("Spark session đã dừng.")

if __name__ == "__main__":
    processing_date_partition = get_target_date_article(sys.argv)
    main(processing_date_partition)