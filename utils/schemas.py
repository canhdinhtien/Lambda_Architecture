from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, DateType
def get_stock_data_schema_from_kafka():
    return StructType([
        StructField("date", StringType(), True),             
        StructField("symbol", StringType(), True),            
        StructField("price_adjusted", DoubleType(), True),   
        StructField("price_close", DoubleType(), True),     
        StructField("price_open", DoubleType(), True),      
        StructField("price_high", DoubleType(), True),       
        StructField("price_low", DoubleType(), True),        
        StructField("change_value", DoubleType(), True),    
        StructField("change_percent", DoubleType(), True),   
        StructField("change_raw", StringType(), True),        
        StructField("volume_matched", LongType(), True),      
        StructField("value_matched", DoubleType(), True),      
        StructField("volume_put_through", LongType(), True),   
        StructField("value_put_through", DoubleType(), True),  
        StructField("processing_timestamp", StringType(), True) 
    ])

def get_processed_stock_schema_for_hdfs():
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("original_date_str", StringType(), True),
        StructField("price_adjusted", DoubleType(), True),   
        StructField("price_close", DoubleType(), True),     
        StructField("price_open", DoubleType(), True),       
        StructField("price_high", DoubleType(), True),       
        StructField("price_low", DoubleType(), True),        
        StructField("change_value", DoubleType(), True),     
        StructField("change_percent", DoubleType(), True),   
        StructField("change_raw", StringType(), True),       
        StructField("volume_matched", LongType(), True),     
        StructField("value_matched", DoubleType(), True),    
        StructField("volume_put_through", LongType(), True),
        StructField("value_put_through", DoubleType(), True),
        StructField("processing_timestamp", StringType(), True), 
        StructField("event_date", DateType(), True),         
    ])

def get_stock_batch_view_schema():
     return StructType([
        StructField("doc_id", StringType(), False), 
        StructField("symbol", StringType(), True),    
        StructField("open", DoubleType(), True),    
        StructField("high", DoubleType(), True),    
        StructField("low", DoubleType(), True),     
        StructField("close", DoubleType(), True),   
        StructField("volume", LongType(), True),      
        StructField("value", DoubleType(), True),     
        StructField("data_date", DateType(), True),  
        StructField("partition_date", StringType(), True), 
        StructField("view_type", StringType(), True),  
        StructField("processing_timestamp", TimestampType(), True) 
     ])

def get_stock_realtime_view_schema():
    return StructType([
        StructField("rt_view_id", StringType(), False), 
        StructField("symbol", StringType(), True),      
        StructField("window_start", TimestampType(), True), 
        StructField("window_end", TimestampType(), True),   
        StructField("avg_price_window", DoubleType(), True),
        StructField("total_volume_window", LongType(), True), 
        StructField("min_price_window", DoubleType(), True), 
        StructField("max_price_window", DoubleType(), True), 
        StructField("record_count_window", LongType(), True),
        StructField("view_type", StringType(), True)       
     ])
