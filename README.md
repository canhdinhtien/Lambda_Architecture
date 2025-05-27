1.  Mở Docker

    - Mở PowerShell trong thư mục gốc của dự án
    - Chạy lệnh:
      - docker-compose up -d
      - docker-compose ps (Đảm bảo cột `State` là `Up` cho tất cả các service).

2.  Setup Hạ tầng (Chạy một lần sau khi các container đã `Up`):

    - Tạo Kafka Topics:
      docker-compose exec kafka bash /scripts/setup_kafka.sh
    - Setup Elasticsearch Indices:
      docker-compose exec elasticsearch bash /scripts/setup_elasticsearch.sh
    - Tạo thư mục HDFS (Cho dữ liệu):
      docker-compose exec namenode hdfs dfs -mkdir -p /user/stock_market/raw
      docker-compose exec namenode hdfs dfs -mkdir -p /user/stock_market/checkpoints/speed_layer
      Tùy chọn (Nếu chạy cả json_producer):
      docker-compose exec namenode hdfs dfs -mkdir -p /user/articles/raw
      docker-compose exec namenode hdfs dfs -mkdir -p /user/articles/checkpoints/speed_layer

3.  Chạy Các Thành phần Ứng dụng (Chứng khoán):

    - 3.1. Chạy Data Producer (CSV to Kafka):

      - Mở cửa sổ PowerShell mới.
      - docker-compose exec spark-master python /app/data_producer/producer.py /app/data_producer/input_data/finance_data.csv --delay 0.05

    - 3.2. Chạy Speed Layer Processor (Kafka to ES Realtime):

      - Mở cửa sổ PowerShell mới.
      - Chạy lệnh sau để khởi động job streaming:
        docker-compose exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 --conf spark.cores.max=1 /app/speed_layer/speed_processor.py
      - Job này sẽ chạy liên tục, đọc dữ liệu mới từ Kafka, xử lý và ghi vào index `stock_market_realtime_views` trên Elasticsearch. Theo dõi log trong cửa sổ này.

    - 3.3. Chạy Batch Layer (Kafka -> HDFS -> ES Batch):

      - Bước 3.3.1: Kafka to HDFS (Stock)

        - Mở cửa sổ PowerShell mới.
        - Chạy lệnh sau để xử lý dữ liệu hiện có trong Kafka và lưu vào HDFS:
          docker-compose exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --conf spark.cores.max=1 /app/batch_layer/kafka_to_hdfs.py

      - Bước 3.3.2: Liệt kê patition_time:
        docker-compose exec namenode hdfs dfs -ls hdfs://namenode:9000/user/stock_market/raw/
      - Bước 3.3.3: Batch Processing (Stock - HDFS to ES)
        docker-compose exec spark-master spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 --conf spark.cores.max=1 /app/batch_layer/batch_processor.py YYYY-MM-DD (thay bằng ngày cụ thể)

4.  Truy cập Giao diện Web

    - Kibana: [http://localhost:5601](http://localhost:5601)
    - Spark Master UI: [http://localhost:8080](http://localhost:8080) (Xem các ứng dụng Spark đang chạy và đã hoàn thành).
    - HDFS NameNode UI: [http://localhost:9870](http://localhost:9870) (Duyệt file trên HDFS).
    - Elasticsearch API: [http://localhost:9200](http://localhost:9200) (Ví dụ: truy cập `http://localhost:9200/_cat/indices?v` để xem danh sách index).

5.  Dừng và Dọn dẹp:

    - Để dừng và xóa tất cả dữ liệu trong volumes (HDFS, Elasticsearch, Kafka data, Checkpoints):
      docker-compose down -v
