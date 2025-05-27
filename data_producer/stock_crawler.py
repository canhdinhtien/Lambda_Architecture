import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import json

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

try:
    from config import settings
    KAFKA_BROKERS = settings.KAFKA_BROKERS
    KAFKA_TOPIC_STOCK = settings.KAFKA_TOPIC_STOCK_DATA
except Exception:
    KAFKA_BROKERS = ['localhost:29092']
    KAFKA_TOPIC_STOCK = 'stock_data'

HDFS_DIR = "hdfs_data"
os.makedirs(HDFS_DIR, exist_ok=True)

def send_to_kafka(records, kafka_brokers, kafka_topic):
    if not KafkaProducer:
        print("KafkaProducer not installed. Skipping Kafka send.")
        return
    producer = KafkaProducer(
        bootstrap_servers=kafka_brokers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3,
    )
    for record in records:
        producer.send(kafka_topic, value=record, key=record.get('Symbol', '').encode('utf-8'))
    producer.flush()
    producer.close()
    print(f"Sent {len(records)} records to Kafka topic '{kafka_topic}'.")

def crawl_stock_data(output_filename="finance_data.parquet", send_kafka=True):
    response = requests.get('https://topi.vn/danh-sach-ma-chung-khoan-theo-nganh-tai-viet-nam.html')
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    symbols = []
    rows = soup.find_all("tr")[1:]

    for row in rows:
        tds = row.find_all("td")
        if len(tds) >= 2:
            code_tag = tds[1].find("p")
            if code_tag:
                code = code_tag.get_text(strip=True)
                symbols.append(code)

    all_data = []

    for symbol in symbols:
        url = f'https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={symbol}&StartDate=&EndDate=&PageIndex=1&PageSize=6003'
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        price_data = json_data.get('Data', {}).get('Data', [])
        for item in price_data:
            item['Symbol'] = symbol
        all_data.extend(price_data)

    df = pd.DataFrame(all_data)
    output_path = os.path.join(HDFS_DIR, output_filename)
    df.to_parquet(output_path, index=False)
    print(f"Stock data saved to {output_path}")

    if send_kafka and len(all_data) > 0:
        send_to_kafka(all_data, KAFKA_BROKERS, KAFKA_TOPIC_STOCK)

if __name__ == "__main__":
    crawl_stock_data()