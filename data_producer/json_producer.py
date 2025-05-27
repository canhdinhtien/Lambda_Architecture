"""Trình tạo dữ liệu từ một tập tin JSON và gửi nó đến Kafka."""
import json
import time
import sys
import os
import argparse
from kafka import KafkaProducer
from datetime import datetime
import hashlib 

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

try:
    from config import settings
except ImportError:
    print("Lỗi: Không thể nhập cài đặt. Đảm bảo config/settings.py tồn tại và PYTHONPATH đúng.")
    sys.exit(1)

def generate_article_id(record):
    """Tạo ID duy nhất cho bài viết dựa trên các trường cố định."""
    url = record.get('url')
    if url:
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    title = record.get('title', '')
    summary = record.get('summary', '')
    combined = title + summary
    if combined:
         return hashlib.sha256(combined.encode('utf-8')).hexdigest()
    return None


def process_json_record(record_dict):
    """Xử lý một dictionary từ dữ liệu JSON (bài viết), chuyển đổi sang định dạng Kafka."""
    processed_data = {}

    article_id = generate_article_id(record_dict)
    if not article_id:

        print(f"Cảnh báo: Không thể tạo article_id cho bản ghi: {record_dict.get('title', 'Không tiêu đề')}. Bỏ qua bản ghi này.")
        return None 

    processed_data['article_id'] = article_id
    processed_data['title'] = record_dict.get('title')
    processed_data['url'] = record_dict.get('url')
    processed_data['summary'] = record_dict.get('summary')
    processed_data['published_time_str'] = record_dict.get('published_time')
    # TODO: 

    try:
        if processed_data['published_time_str']:
            processed_data['published_timestamp'] = datetime.strptime(processed_data['published_time_str'], '%Y-%m-%d %H:%M:%S').isoformat() + 'Z' 
        else:
            processed_data['published_timestamp'] = None
    except ValueError:
        processed_data['published_timestamp'] = None

    processed_data['image_url'] = record_dict.get('image_url')
    processed_data['full_content'] = record_dict.get('full_content')
    processed_data['content_method'] = record_dict.get('content_method')

    processed_data['processing_timestamp'] = datetime.now().isoformat()

    return processed_data


def main_json(json_file_path, delay_seconds):
    """Hàm chính đọc JSON (bài viết) và gửi dữ liệu lên Kafka."""
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


    print(f"Bắt đầu trình tạo dữ liệu JSON cho chủ đề '{settings.KAFKA_TOPIC_ARTICLES}'...") 
    print(f"Đang đọc file JSON: {json_file_path}")
    print(f"Delay giữa các bản ghi: {delay_seconds} giây")

    record_count = 0
    start_time = time.time()

    try:
        records = []
        with open(json_file_path, mode='r', encoding='utf-8') as jsonfile:
            print(f"Đang đọc toàn bộ nội dung file JSON: {json_file_path}")
            data = json.load(jsonfile)

        if not isinstance(data, list):
            print("Lỗi: File JSON không chứa một mảng dữ liệu gốc.")
            sys.exit(1)

        records = data 
        print(f"Đã đọc thành công {len(records)} bản ghi từ file JSON.")


        for record in records:
                try:
                    processed_data = process_json_record(record)

                    if processed_data is None:
                        continue

                    kafka_key = processed_data.get('article_id', None)

                    if not kafka_key:
                        print(f"Cảnh báo: Bỏ qua bản ghi JSON do thiếu Key (article_id) sau xử lý: {record}")
                        continue

                    future = producer.send(settings.KAFKA_TOPIC_ARTICLES,
                                        value=processed_data,
                                        key=str(kafka_key).encode('utf-8'))

                    producer.flush(timeout=5)

                    record_count += 1

                    if record_count % 100 == 0:
                        elapsed_time = time.time() - start_time
                        print(f"Sent {record_count} JSON records (articles) in {elapsed_time:.2f} seconds.")

                    time.sleep(delay_seconds)

                except Exception as e:
                    print(f"Lỗi khi xử lý/gửi bản ghi JSON: {record}. Lỗi: {e}")
                    time.sleep(1) 

    except FileNotFoundError:
        print(f"Lỗi: Không thể mở file JSON: {json_file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"LỖI: Không thể phân tích cú pháp toàn bộ file JSON: {json_file_path}. Đảm bảo file chứa một mảng JSON hợp lệ. Lỗi: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Đã xảy ra lỗi không mong muốn: {e}")
    finally:
        if producer:
            print(f"Đã gửi {record_count} bản ghi JSON (articles). Đang xóa các thông điệp cuối cùng...")
            producer.flush()
            producer.close()
            print("Hoàn tất JSON producer (articles).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka producer to read data from JSON file (articles).')
    parser.add_argument('json_file', help='Path to the input JSON file (articles)')
    parser.add_argument('--delay', type=float, default=0.1,
                        help='Delay in seconds between sending records (default: 0.1)')
    args = parser.parse_args()
    main_json(args.json_file, args.delay)