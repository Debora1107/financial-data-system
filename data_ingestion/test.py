import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger

from data_fetcher import YahooFinanceFetcher
from message_publisher import MessagePublisher

logger = get_logger("data_ingestion_test")

def test_data_fetcher():
    """Test the data fetcher functionality."""
    logger.info("Testing data fetcher...")
    
    fetcher = YahooFinanceFetcher()
    
    logger.info("Testing fetch_historical_data...")
    historical_data = fetcher.fetch_historical_data("AAPL", period="5d", interval="1d")
    if historical_data:
        logger.info(f"Successfully fetched {len(historical_data)} records for AAPL")
        logger.info(f"Sample data: {historical_data[0] if historical_data else None}")
    else:
        logger.error("Failed to fetch historical data for AAPL")
    
    logger.info("Testing fetch_real_time_data...")
    real_time_data = fetcher.fetch_real_time_data("MSFT")
    if real_time_data:
        logger.info(f"Successfully fetched real-time data for MSFT")
        logger.info(f"Sample data: {real_time_data}")
    else:
        logger.error("Failed to fetch real-time data for MSFT")
    
    logger.info("Testing fetch_multiple_symbols...")
    multi_data = fetcher.fetch_multiple_symbols(["AAPL", "MSFT"], period="1d")
    if multi_data:
        logger.info(f"Successfully fetched data for multiple symbols")
        logger.info(f"Symbols: {list(multi_data.keys())}")
    else:
        logger.error("Failed to fetch data for multiple symbols")
    
    logger.info("Data fetcher tests completed")

def test_message_publisher():
    """Test the message publisher functionality."""
    logger.info("Testing message publisher...")
    
    fetcher = YahooFinanceFetcher()
    publisher = MessagePublisher()
    
    if publisher.connect():
        logger.info("Successfully connected to RabbitMQ")
        
        logger.info("Testing publish_historical_data...")
        apple_data = fetcher.fetch_historical_data("AAPL", period="1d")
        if publisher.publish_historical_data("AAPL", apple_data):
            logger.info("Successfully published historical data for AAPL")
        else:
            logger.error("Failed to publish historical data for AAPL")
        
        logger.info("Testing publish_real_time_data...")
        msft_data = fetcher.fetch_real_time_data("MSFT")
        if publisher.publish_real_time_data(msft_data):
            logger.info("Successfully published real-time data for MSFT")
        else:
            logger.error("Failed to publish real-time data for MSFT")
        
        publisher.close()
        logger.info("Closed connection to RabbitMQ")
    else:
        logger.warning("Failed to connect to RabbitMQ - skipping message publisher tests")
    
    logger.info("Message publisher tests completed")

def run_tests():
    """Run all tests."""
    logger.info("Starting Data Ingestion Service tests...")
    
    test_data_fetcher()
    
    test_message_publisher()
    
    logger.info("All tests completed")

if __name__ == "__main__":
    run_tests() 