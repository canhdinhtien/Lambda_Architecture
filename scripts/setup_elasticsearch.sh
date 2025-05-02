
ES_HOST="elasticsearch" 
ES_PORT="9200"


ES_SMARTGRID_BATCH_INDEX="smartgrid_batch_views"
ES_SMARTGRID_REALTIME_INDEX="smartgrid_realtime_views"

ES_STOCK_BATCH_INDEX="stock_market_batch_views" 

MAX_WAIT_SECONDS=120
WAIT_INTERVAL=5
ELAPSED_TIME=0

echo "Waiting up to $MAX_WAIT_SECONDS seconds for Elasticsearch to be ready at http://${ES_HOST}:${ES_PORT}..."


until curl -s "http://${ES_HOST}:${ES_PORT}/_cluster/health?wait_for_status=yellow&timeout=1s" > /dev/null; do
    if [ $ELAPSED_TIME -ge $MAX_WAIT_SECONDS ]; then
        echo "ERROR: Elasticsearch did not become ready within $MAX_WAIT_SECONDS seconds."
        exit 1
    fi
    echo "Still waiting for Elasticsearch..."
    sleep $WAIT_INTERVAL
    ELAPSED_TIME=$((ELAPSED_TIME + WAIT_INTERVAL))
done

echo "Elasticsearch is ready. Setting up index templates..."


echo "Creating/Updating Smart Grid Batch View Index Template: ${ES_SMARTGRID_BATCH_INDEX}_template"
curl -s -X PUT "http://${ES_HOST}:${ES_PORT}/_index_template/${ES_SMARTGRID_BATCH_INDEX}_template" -H 'Content-Type: application/json' -d'
{
  "index_patterns": ["'"${ES_SMARTGRID_BATCH_INDEX}"'*"],
  "priority": 500,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "view_id": { "type": "keyword" },          # Giả sử đây là ID document
        "meter_id": { "type": "keyword" },
        "date": { "type": "date", "format": "yyyy-MM-dd" },
        "total_daily_kwh": { "type": "double" },
        "avg_daily_voltage": { "type": "float" },
        "view_type": { "type": "keyword" },
        "processing_timestamp": { "type": "date"} # Thường là ISO 8601
      }
    }
  },
  "_meta": { "description": "Template for smart grid batch views" }
}'
echo "" 


echo "Creating/Updating Smart Grid Realtime View Index Template: ${ES_SMARTGRID_REALTIME_INDEX}_template"
curl -s -X PUT "http://${ES_HOST}:${ES_PORT}/_index_template/${ES_SMARTGRID_REALTIME_INDEX}_template" -H 'Content-Type: application/json' -d'
{
  "index_patterns": ["'"${ES_SMARTGRID_REALTIME_INDEX}"'*"],
  "priority": 500,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "view_id": { "type": "keyword" },          # Giả sử đây là ID document
        "meter_id": { "type": "keyword" },
        "window_start": { "type": "date" },       # Thường là ISO 8601
        "window_end": { "type": "date" },         # Thường là ISO 8601
        "total_consumption_window": { "type": "double" },
        "avg_voltage_window": { "type": "float" },
        "record_count_window": { "type": "integer" },
        "view_type": { "type": "keyword" }
      }
    }
  },
  "_meta": { "description": "Template for smart grid real-time views" }
}'
echo "" 


echo "Creating/Updating Stock Market Batch View Index Template: ${ES_STOCK_BATCH_INDEX}_template"
curl -s -X PUT "http://${ES_HOST}:${ES_PORT}/_index_template/${ES_STOCK_BATCH_INDEX}_template" -H 'Content-Type: application/json' -d'
{
  "index_patterns": ["'"${ES_STOCK_BATCH_INDEX}"'*"],
  "priority": 500,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "doc_id": { "type": "keyword" },          # ID document (symbol_partition_date)
        "symbol": { "type": "keyword" },          # Mã chứng khoán
        "open": { "type": "float" },             # Giá mở cửa
        "high": { "type": "float" },             # Giá cao nhất
        "low": { "type": "float" },              # Giá thấp nhất
        "close": { "type": "float" },            # Giá đóng cửa
        "volume": { "type": "long" },             # Khối lượng khớp lệnh (có thể rất lớn)
        "value": { "type": "double" },           # Giá trị khớp lệnh
        "data_date": { "type": "date", "format": "yyyy-MM-dd" }, # Ngày dữ liệu gốc (nếu có, đã parse)
        "partition_date": { "type": "date", "format": "yyyy-MM-dd" }, # Ngày partition xử lý
        "view_type": { "type": "keyword" },       # 'batch'
        "processing_timestamp": { "type": "date" } # Thời điểm xử lý (thường là ISO 8601)
        # Thêm các trường khác từ batch_processor nếu bạn tính toán thêm
      }
    }
  },
  "_meta": { "description": "Template for stock market batch views" }
}'
echo "" 


echo "Elasticsearch setup script finished."
exit 0 