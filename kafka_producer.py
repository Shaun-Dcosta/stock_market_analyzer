import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
TICKERS = ['AAPL', 'MSFT', 'TSLA']
KAFKA_TOPIC = 'stock-prices-raw'
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_stock_data(symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}&outputsize=compact'
    response = requests.get(url)
    data = response.json()
    try:
        latest_time = list(data['Time Series (1min)'].keys())[0]
        price_info = data['Time Series (1min)'][latest_time]
        return {
            'symbol': symbol,
            'timestamp': latest_time,
            'price': float(price_info['1. open']),
            'volume': int(price_info['5. volume'])
        }
    except KeyError:
        print(f"API limit reached or error for {symbol}: {data}")
        return None

while True:
    for ticker in TICKERS:
        stock_data = fetch_stock_data(ticker)
        if stock_data:
            print(f"Sending to Kafka: {stock_data}")
            producer.send(KAFKA_TOPIC, stock_data)
    time.sleep(60)  # Respect Alpha Vantage rate limits (5 calls/min for free tier)
