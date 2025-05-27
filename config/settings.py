import os

KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
HDFS_NAMENODE = os.environ.get('HDFS_NAMENODE', 'hdfs://namenode:9000') 
ES_HOSTS = os.environ.get('ES_HOSTS', 'elasticsearch') 
ES_PORT = os.environ.get('ES_PORT', '9200') 
ES_URL = f"http://{ES_HOSTS}:{ES_PORT}" 
SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://spark-master:7077')

KAFKA_TOPIC_STOCK_DATA = 'stock_market_data'

HDFS_PATH_STOCK_BASE = f"{HDFS_NAMENODE}/user/stock_market" 
HDFS_PATH_STOCK_RAW = f"{HDFS_PATH_STOCK_BASE}/raw"
HDFS_PATH_CHECKPOINTS_SPEED_STOCK = f"{HDFS_PATH_STOCK_BASE}/checkpoints/speed_layer"

ES_INDEX_STOCK_BATCH_VIEWS = 'stock_market_batch_views'
ES_INDEX_STOCK_REALTIME_VIEWS = 'stock_market_realtime_views'

ES_DOC_ID_FIELD_STOCK_BATCH = 'doc_id'
ES_DOC_ID_FIELD_STOCK_RT = 'rt_view_id' 

SPARK_PACKAGES_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1" 

SPARK_PACKAGES_ES = "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1"

BATCH_TARGET_DATE_FORMAT = "yyyy-MM-dd"
HDFS_NAMENODE_URI = "hdfs://namenode:9000"

print("--- Configuration Loaded ---")
print(f"Kafka Brokers: {KAFKA_BROKERS}")
print(f"HDFS NameNode: {HDFS_NAMENODE}")
print(f"Elasticsearch Host: {ES_HOSTS}:{ES_PORT}")
print(f"Spark Master: {SPARK_MASTER}")
print(f"Stock Kafka Topic: {KAFKA_TOPIC_STOCK_DATA}")
print(f"Stock HDFS Raw Path: {HDFS_PATH_STOCK_RAW}")
print(f"Stock ES Batch Index: {ES_INDEX_STOCK_BATCH_VIEWS}")
print(f"Stock ES Realtime Index: {ES_INDEX_STOCK_REALTIME_VIEWS}")
print(f"Batch Date Format: {BATCH_TARGET_DATE_FORMAT}")
print("-----------------------------")