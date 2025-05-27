import requests
from bs4 import BeautifulSoup
import time
import random
import os
import json
from urllib.parse import urljoin
from newspaper import Article
import pandas as pd
from kafka import KafkaProducer

try:
    from config import settings
    KAFKA_BROKERS = settings.KAFKA_BROKERS
    KAFKA_TOPIC_NEWS = settings.KAFKA_TOPIC_NEWS
except Exception:
    KAFKA_BROKERS = ['localhost:29092']
    KAFKA_TOPIC_NEWS = 'news_data'

BASE_URL = 'https://vnexpress.net'
HDFS_DIR = 'hdfs_data'
os.makedirs(HDFS_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://vnexpress.net/',
    'DNT': '1',
    'Connection': 'keep-alive',
}

def extract_article_content_advanced(url, max_retries=2):
    # Phương pháp 1: Sử dụng newspaper3k
    for attempt in range(max_retries):
        try:
            article = Article(url, language='vi')
            article.download()
            article.parse()

            if article.text and len(article.text.strip()) > 100:  
                return {
                    "title": article.title,
                    "text": article.text,
                    "url": url,
                    "published": article.publish_date.isoformat() if article.publish_date else None,
                    "method": "newspaper3k"
                }
        except Exception as e:
            print(f"🔄 Newspaper3k attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(1)

    # Phương pháp 2: Parse trực tiếp với BeautifulSoup
    try:
        print(f"📖 Fallback to manual parsing for: {url}")
        time.sleep(random.uniform(1, 2))

        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        content_selectors = [
            '.fck_detail',  
            '.Normal',
            'article .content',
            '.content-detail',
            '.article-content'
        ]

        article_content = ""
        title = ""

        # Lấy tiêu đề
        title_selectors = ['h1.title-detail', 'h1', '.title-news', '.title']
        for selector in title_selectors:
            title_tag = soup.select_one(selector)
            if title_tag:
                title = title_tag.get_text(strip=True)
                break

        # Lấy nội dung
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                # Loại bỏ các element không cần thiết
                for unwanted in content_div.select('script, style, .ads, .advertisement, .social-share'):
                    unwanted.decompose()

                paragraphs = content_div.find_all(['p', 'div'], class_=lambda x: x != 'ads' if x else True)
                article_content = os.linesep.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])

                if len(article_content.strip()) > 100:
                    break

        if article_content and len(article_content.strip()) > 100:
            return {
                "title": title,
                "text": article_content,
                "url": url,
                "published": None,
                "method": "manual_parsing"
            }

    except Exception as e:
        print(f"Phân tích không thành công {url}: {e}")

    print(f"Không thể lấy nội dung từ {url}")
    return None

def send_article_to_kafka(article, producer, topic):
    try:
        producer.send(topic, value=article, key=article.get('url', '').encode('utf-8'))
        producer.flush()
        print(f"Đã gửi bài: {article.get('title', '')[:50]}... vào Kafka")
    except Exception as e:
        print(f"Lỗi gửi Kafka: {e}")

def crawl_vnexpress_category(category_path, pages=3, delay_range=(2, 4), producer=None, kafka_topic=None):
    print(f"Bắt đầu crawl: {category_path}")
    articles_collected = []

    for page in range(1, pages + 1):
        if page == 1:
            url = f"{BASE_URL}/{category_path}"
        else:
            url = f"{BASE_URL}/{category_path}-p{page}"

        try:
            print(f"Trang {page}: {url}")
            time.sleep(random.uniform(*delay_range))

            res = requests.get(url, headers=HEADERS, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.content, 'html.parser')

            # Nhiều selector khác nhau cho VnExpress
            article_selectors = [
                'article.item-news',
                '.item-news',
                '.list-news-subfolder article',
                '.item-news-common',
                'article',
                '.title-news'
            ]

            articles_found = []
            for selector in article_selectors:
                found = soup.select(selector)
                if found:
                    articles_found.extend(found)
                    print(f"Tìm thấy {len(found)} bài với selector: {selector}")

            # Loại bỏ duplicate
            unique_articles = []
            seen_urls = set()

            for article in articles_found:
                try:
                    # Tìm link bài viết
                    link_selectors = ['h3 a', 'h2 a', 'a.title-news', 'a[href*=".html"]']
                    title = None
                    link = None

                    for link_sel in link_selectors:
                        title_tag = article.select_one(link_sel)
                        if title_tag:
                            title = title_tag.get_text(strip=True)
                            link = title_tag.get('href')
                            break

                    if not link:
                        continue

                    if not link.startswith('http'):
                        link = urljoin(BASE_URL, link)

                    if link in seen_urls:
                        continue
                    seen_urls.add(link)

                    summary_tag = article.select_one('p.description, .description, p')
                    time_tag = article.select_one('.time, .date, time')
                    image_tag = article.select_one('img')

                    article_data = {
                        'title': title,
                        'url': link,
                        'summary': summary_tag.get_text(strip=True) if summary_tag else '',
                        'published_time': time_tag.get_text(strip=True) if time_tag else '',
                        'image_url': image_tag.get('src') if image_tag else '',
                        'full_content': '',
                        'content_method': ''
                    }

                    unique_articles.append(article_data)

                except Exception as e:
                    print(f"Lỗi xử lý bài viết: {e}")
                    continue

            print(f"Tổng cộng {len(unique_articles)} bài unique")

            # Lấy full content cho từng bài
            for idx, article_data in enumerate(unique_articles, 1):
                print(f"Lấy nội dung ({idx}/{len(unique_articles)}): {article_data['title'][:50]}...")

                content = extract_article_content_advanced(article_data['url'])
                if content:
                    article_data['full_content'] = content['text']
                    article_data['content_method'] = content['method']
                    print(f"Thành công ({content['method']}) - {len(content['text'])} ký tự")
                else:
                    print(f"Thất bại")

                articles_collected.append(article_data)

                # Gửi vào Kafka nếu có producer
                if producer and kafka_topic:
                    send_article_to_kafka(article_data, producer, kafka_topic)

                time.sleep(random.uniform(1, 2))

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error {e.response.status_code} - {url}")
            if e.response.status_code == 429:  # Too Many Requests
                print("Đang bị rate limit, đợi lâu hơn...")
                time.sleep(30)
            continue
        except Exception as e:
            print(f"Other Error - {e}")
            continue

    print(f"Hoàn thành! Tổng cộng: {len(articles_collected)} bài viết")
    return articles_collected

def main():
    categories = [
        "kinh-doanh/chung-khoan",
        "kinh-doanh/tai-chinh",
        "kinh-doanh/quoc-te",
        "kinh-doanh/doanh-nghiep",
        "kinh-doanh/ebank",
        "kinh-doanh/vi-mo",
        "kinh-doanh/tien-cua-toi",
        "kinh-doanh/hang-hoa"
    ]

    # Khởi tạo Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3,
    )

    all_articles_combined = []

    for idx, cat_path in enumerate(categories, 1):
        print(f"\n{'='*60}")
        print(f"🎯 CRAWLING CATEGORY ({idx}/{len(categories)}): {cat_path}")
        print(f"{'='*60}")

        articles_from_category = crawl_vnexpress_category(
            cat_path, pages=20, producer=producer, kafka_topic=KAFKA_TOPIC_NEWS
        )

        if articles_from_category:
            all_articles_combined.extend(articles_from_category)
            print(f"Đã thu thập {len(articles_from_category)} bài từ chuyên mục {cat_path}")
        else:
            print(f"Không thu thập được bài viết nào từ chuyên mục {cat_path}")

        if idx < len(categories):  
            print(f"⏱️ Nghỉ 15 giây trước khi chuyển category tiếp theo...")
            time.sleep(15)

    try:
        df = pd.DataFrame(all_articles_combined)
        output_path = os.path.join(HDFS_DIR, 'all_vnexpress_combined_articles_with_content.parquet')
        df.to_parquet(output_path, index=False, engine='fastparquet')

        print(f"\nĐã lưu tổng cộng {len(all_articles_combined)} bài viết từ tất cả chuyên mục vào file '{output_path}'")

    except Exception as e:
        print(f"\nLỗi khi lưu dữ liệu kết hợp vào file JSON: {e}")

    producer.close()

if __name__ == "__main__":
    main()