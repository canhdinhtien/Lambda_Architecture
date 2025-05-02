from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import os
import sys
from datetime import datetime, timedelta

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

try:
    from config import settings
except ImportError:
    print("Lỗi: Không thể nhập settings. Đảm bảo config/settings.py tồn tại và PYTHONPATH đúng.")
    sys.exit(1)


def get_target_date(args):
    """Xác định ngày xử lý mục tiêu từ tham số dòng lệnh hoặc mặc định là ngày hôm qua."""
    if len(args) > 1:
        try:
            target_dt = datetime.strptime(args[1], '%Y-%m-%d')
            print(f"Đang xử lý dữ liệu cho ngày partition từ tham số: {args[1]}")
            return args[1] 
        except ValueError:
            print(f"Cảnh báo: Định dạng ngày không hợp lệ: {args[1]}. Định dạng mong muốn là YYYY-MM-DD.")
    yesterday = datetime.now() - timedelta(days=1)
    default_date = yesterday.strftime('%Y-%m-%d')
    print(f"Không có tham số ngày hợp lệ, sử dụng partition của ngày hôm qua: {default_date}")
    return default_date

def main(target_date_partition):
    print(f"Đang khởi tạo Spark Session cho Xử lý Batch Chứng khoán (Partition Ngày: {target_date_partition})...")

    if not hasattr(settings, 'SPARK_PACKAGES_ES'):
        print("LỖI: settings.SPARK_PACKAGES_ES chưa được định nghĩa trong config/settings.py")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName(f"XuLyBatchChungKhoan_{target_date_partition}") \
        .master(settings.SPARK_MASTER) \
        .config("spark.jars.packages", settings.SPARK_PACKAGES_ES) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Session đã được tạo. Master: {settings.SPARK_MASTER}")

    try:
        if not hasattr(settings, 'HDFS_PATH_STOCK_RAW'):
             print("LỖI: settings.HDFS_PATH_STOCK_RAW chưa được định nghĩa trong config/settings.py")
             sys.exit(1)

        input_path = os.path.join(settings.HDFS_PATH_STOCK_RAW, f"partition_date={target_date_partition}")
        print(f"Đang đọc dữ liệu Chứng khoán từ đường dẫn HDFS Parquet: {input_path}")

        try:
            df = spark.read.parquet(input_path)
            print(f"Đọc thành công dữ liệu Parquet cho partition ngày {target_date_partition}. Schema:")
            df.printSchema()
        except AnalysisException as e:
             if "Path does not exist" in str(e):
                  print(f"LỖI: Đường dẫn đầu vào không tồn tại: {input_path}. Không tìm thấy dữ liệu cho partition {target_date_partition}. Đảm bảo tiến trình trước đó đã chạy đúng.")
                  return
             else:
                  raise e

        print("Đang tính toán bản tóm tắt chứng khoán hàng ngày (batch view)...")

        date_col_to_use = "event_date" if "event_date" in df.columns else "original_date_str"

        daily_summary = df.groupBy("symbol") \
                          .agg(
                              F.first("price_open").alias("open"),
                              F.first("price_high").alias("high"),
                              F.first("price_low").alias("low"),
                              F.first("price_close").alias("close"),
                              F.sum("volume_matched").alias("volume"),
                              F.sum("value_matched").alias("value"), 
                           )

        batch_view_intermediate = daily_summary \
                                .withColumn("partition_date", F.lit(target_date_partition)) \
                                .withColumn("view_type", F.lit("batch")) \
                                .withColumn("processing_timestamp", F.current_timestamp())

        if not hasattr(settings, 'ES_INDEX_STOCK_BATCH_VIEWS'):
             print("LỖI: settings.ES_INDEX_STOCK_BATCH_VIEWS chưa được định nghĩa trong config/settings.py")
             sys.exit(1)
        if not hasattr(settings, 'ES_DOC_ID_FIELD_STOCK_BATCH'):
            print("LỖI: settings.ES_DOC_ID_FIELD_STOCK_BATCH chưa được định nghĩa trong config/settings.py")
            sys.exit(1)

        doc_id_col = settings.ES_DOC_ID_FIELD_STOCK_BATCH 
        stock_batch_view = batch_view_intermediate.withColumn(
            doc_id_col,
            F.concat(F.col("symbol"), F.lit("_"), F.col("partition_date"))
        )

        print("Kết quả Stock Batch View đã tính toán:")
        stock_batch_view.show(10, truncate=False)

        es_resource = settings.ES_INDEX_STOCK_BATCH_VIEWS
        es_write_options = {
            "es.nodes": settings.ES_HOSTS,
            "es.port": settings.ES_PORT,
            "es.resource": es_resource,
            "es.mapping.id": doc_id_col,
            "es.write.operation": "index",
            "es.nodes.wan.only": "true"
        }
        print(f"Đang ghi stock batch view vào Elasticsearch: {es_resource} trên nodes {settings.ES_HOSTS}:{settings.ES_PORT}")

        stock_batch_view.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_options) \
            .mode("append") \
            .save()

        print(f"Đã ghi thành công stock batch view cho partition ngày {target_date_partition} vào Elasticsearch.")

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý batch chứng khoán cho partition {target_date_partition}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Đang dừng Spark session.")
        if 'spark' in locals() and spark:
             spark.stop()
             print("Spark session đã dừng.")

if __name__ == "__main__":
    processing_date_partition = get_target_date(sys.argv)
    main(processing_date_partition)