import sys
import os
import json
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger

from data_processor import DataProcessor
from message_consumer import MessageConsumer

logger = get_logger("real_time_processing_test")

def test_data_processor():
    """Test the data processor functionality."""
    logger.info("Testing data processor...")
    
    processor = DataProcessor()
    
    logger.info("Testing process_historical_data...")
    historical_data = [
        {
            "symbol": "AAPL",
            "date": "2023-01-01T00:00:00",
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 153.0,
            "volume": 1000000
        },
        {
            "symbol": "AAPL",
            "date": "2023-01-02T00:00:00",
            "open": 153.0,
            "high": 158.0,
            "low": 152.0,
            "close": 157.0,
            "volume": 1200000
        }
    ]
    processed_historical = processor.process_historical_data("AAPL", historical_data)
    if processed_historical:
        logger.info(f"Successfully processed historical data")
        logger.info(f"Sample processed data: {processed_historical[0] if processed_historical else None}")
    else:
        logger.error("Failed to process historical data")
    
    logger.info("Testing process_realtime_data...")
    realtime_data = {
        "symbol": "MSFT",
        "timestamp": "2023-01-03T12:34:56",
        "price": 350.0,
        "change": 5.0,
        "change_percent": 1.45,
        "volume": 500000,
        "market_cap": 2600000000000
    }
    processed_realtime = processor.process_realtime_data(realtime_data)
    if processed_realtime:
        logger.info(f"Successfully processed real-time data")
        logger.info(f"Processed data: {processed_realtime}")
    else:
        logger.error("Failed to process real-time data")
    
    logger.info("Data processor tests completed")

def test_message_consumer():
    """Test the message consumer functionality."""
    logger.info("Testing message consumer...")
    
    processor = DataProcessor()
    consumer = MessageConsumer(processor)
    
    logger.info("Testing connect...")
    if consumer.connect():
        logger.info("Successfully connected to RabbitMQ")
        
        logger.info("Testing start_consuming_in_thread and stop_consuming...")
        consumer.start_consuming_in_thread()
        time.sleep(2)  
        
        if consumer.consumer_thread and consumer.consumer_thread.is_alive():
            logger.info("Consumer thread is running")
        else:
            logger.error("Consumer thread is not running")
        
        consumer.stop_consuming()
        time.sleep(2)  
        
        if not consumer.consumer_thread or not consumer.consumer_thread.is_alive():
            logger.info("Consumer thread has stopped")
        else:
            logger.error("Consumer thread is still running")
    else:
        logger.warning("Failed to connect to RabbitMQ - skipping message consumer tests")
    
    logger.info("Message consumer tests completed")

def run_tests():
    """Run all tests."""
    logger.info("Starting Real-Time Processing Service tests...")
    
    test_data_processor()
    
    test_message_consumer()
    
    logger.info("All tests completed")

if __name__ == "__main__":
    run_tests() 