import os
import time
import random
from stock_crawler import crawl_stock_data
from news_crawler import main as crawl_news_data

if __name__ == "__main__":
    print("Starting the crawling process...")

    print("Crawling stock data...")
    crawl_stock_data()
    print("Stock data crawling complete.")

    print("Crawling news data...")
    crawl_news_data()
    print("News data crawling complete.")

    print("Crawling process finished.")
