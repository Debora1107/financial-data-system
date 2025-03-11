import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def create_sample_data(symbol):
    """Create sample financial data for testing."""
    return {
        "symbol": symbol,
        "price": round(random.uniform(50, 200), 2),
        "volume": random.randint(1000, 10000),
        "timestamp": datetime.now().isoformat(),
        "market_cap": random.randint(1000000, 10000000000),
        "shares_outstanding": random.randint(1000000, 100000000),
        "change_percent": round(random.uniform(-5, 5), 2),
        "sentiment": random.choice(["very_positive", "positive", "neutral", "negative", "very_negative"])
    }

def main():
    """Main function to send test data to Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print("Successfully connected to Kafka")
        
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
        
        for _ in range(10):
            for symbol in symbols:
                data = create_sample_data(symbol)
                producer.send('financial_data', data)
                print(f"Sent data for {symbol}: {data}")
                time.sleep(1)  
            
            producer.flush()
            print("Flushed all messages")
        
        producer.close()
        print("Closed Kafka producer")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 