
"""Trình tạo dữ liệu từ một tập tin CSV và gửi nó đến Kafka."""
import json
import time
import sys
import os
import csv  
import argparse 
from kafka import KafkaProducer
from datetime import datetime

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

try:
    from config import settings
except ImportError:
    print("Lỗi: Không thể nhập cài đặt. Đảm bảo config/settings.py tồn tại và PYTHONPATH đúng.")
    sys.exit(1)

def clean_numeric(value_str, data_type=float):
    """Chuyển đổi chuỗi thành số, trả về None nếu lỗi hoặc rỗng."""
    if not value_str:
        return None
    try:
        cleaned_str = value_str.replace(',', '')
        return data_type(cleaned_str)
    except (ValueError, TypeError):
        return None

def parse_thay_doi(value_str):
    """Tách giá trị thay đổi và tỷ lệ phần trăm (nếu cần)."""
    if not value_str or '(' not in value_str:
        return None, None
    try:
        change_part = value_str.split('(')[0]
        percentage_part = value_str.split('(')[1].replace(' %)','').replace('%','')
        return clean_numeric(change_part), clean_numeric(percentage_part)
    except Exception:
        return None, None 

def process_csv_row(row_dict):
    processed_data = {}
    processed_data['date'] = row_dict.get('Ngay') 
    processed_data['symbol'] = row_dict.get('Symbol')
    processed_data['price_adjusted'] = clean_numeric(row_dict.get('GiaDieuChinh'))
    processed_data['price_close'] = clean_numeric(row_dict.get('GiaDongCua'))
    processed_data['price_open'] = clean_numeric(row_dict.get('GiaMoCua'))
    processed_data['price_high'] = clean_numeric(row_dict.get('GiaCaoNhat'))
    processed_data['price_low'] = clean_numeric(row_dict.get('GiaThapNhat'))

    change_val, change_pct = parse_thay_doi(row_dict.get('ThayDoi'))
    processed_data['change_value'] = change_val
    processed_data['change_percent'] = change_pct
    processed_data['change_raw'] = row_dict.get('ThayDoi') 

    processed_data['volume_matched'] = clean_numeric(row_dict.get('KhoiLuongKhopLenh'), int)
    processed_data['value_matched'] = clean_numeric(row_dict.get('GiaTriKhopLenh'))
    processed_data['volume_put_through'] = clean_numeric(row_dict.get('KLThoaThuan'), int)
    processed_data['value_put_through'] = clean_numeric(row_dict.get('GtThoaThuan'))

    processed_data['processing_timestamp'] = datetime.now().isoformat()

    return {k: v for k, v in processed_data.items() if v is not None}

def main(csv_file_path, delay_seconds):
    """Hàm chính đọc CSV và gửi dữ liệu lên Kafka."""
    producer = None
    print(f"Đang cố gắng kết nối tới Kafka brokers...: {settings.KAFKA_BROKERS}")
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
        )
        print("Kết nối thành công tới Kafka.")
    except Exception as e:
        print(f"Lỗi: Không thể kết nối tới Kafka brokers tại {settings.KAFKA_BROKERS}. Lỗi: {e}")
        sys.exit(1)

    print(f"Bắt đầu trình tạo dữ liệu CSV cho chủ đề '{settings.KAFKA_TOPIC_STOCK_DATA}'...")
    print(f"Đang đọc file CSV: {csv_file_path}")
    print(f"Delay giữa các bản ghi: {delay_seconds} giây")

    record_count = 0
    start_time = time.time()

    try:
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile) 
            print(f"CSV Header: {reader.fieldnames}")

            for row in reader:
                try:
                    processed_data = process_csv_row(row)
                    kafka_key = processed_data.get('symbol', None) 

                    if not kafka_key:
                         print(f"Cảnh báo: Bỏ qua dòng do thiếu Symbol: {row}")
                         continue

                    future = producer.send(settings.KAFKA_TOPIC_STOCK_DATA,
                                           value=processed_data,
                                           key=kafka_key.encode('utf-8'))
                    producer.flush(timeout=5) 
                    record_count += 1

                    if record_count % 100 == 0:
                        elapsed_time = time.time() - start_time
                        print(f"Sent {record_count} records in {elapsed_time:.2f} seconds.")

                    time.sleep(delay_seconds)

                except Exception as e:
                    print(f"Lỗi khi xử lý/gửi dòng: {row}. Lỗi: {e}")
                    time.sleep(1) 

    except FileNotFoundError:
        print(f"Lỗi: Không thể mở file CSV: {csv_file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Đã xảy ra lỗi không mong muốn: {e}")
    finally:
        if producer:
            print(f"Đã gửi {record_count} bản ghi. Đang xóa các thông điệp cuối cùng...")
            producer.flush()
            producer.close()
            print("Hoàn tất.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka producer to read data from CSV.')
    parser.add_argument('csv_file', help='Path to the input CSV file')
    parser.add_argument('--delay', type=float, default=0.1,
                        help='Delay in seconds between sending records (default: 0.1)')
    args = parser.parse_args()
    main(args.csv_file, args.delay)