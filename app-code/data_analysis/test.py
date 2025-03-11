import sys
import os
import json
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger

from analyzer import Analyzer

logger = get_logger("data_analysis_test")

def test_analyzer():
    """Test the analyzer functionality."""
    logger.info("Testing analyzer...")
    
    analyzer = Analyzer()
    
    logger.info("Testing get_historical_data...")
    df = analyzer.get_historical_data("AAPL", limit=30)
    if not df.empty:
        logger.info(f"Successfully retrieved historical data with {len(df)} records")
        logger.info(f"Columns: {df.columns.tolist()}")
    else:
        logger.error("Failed to retrieve historical data")
    
    logger.info("Testing get_realtime_data...")
    realtime_data = analyzer.get_realtime_data("AAPL")
    if realtime_data:
        logger.info(f"Successfully retrieved real-time data")
        logger.info(f"Data: {realtime_data}")
    else:
        logger.error("Failed to retrieve real-time data")
    
    logger.info("Testing calculate_technical_indicators...")
    if not df.empty:
        df_with_indicators = analyzer.calculate_technical_indicators(df)
        new_columns = set(df_with_indicators.columns) - set(df.columns)
        logger.info(f"Successfully calculated technical indicators")
        logger.info(f"New columns: {new_columns}")
    else:
        logger.error("Skipping technical indicators test due to missing historical data")
    
    logger.info("Testing predict_price...")
    prediction = analyzer.predict_price("AAPL", days=5)
    if "error" not in prediction:
        logger.info(f"Successfully predicted price")
        logger.info(f"Prediction: {json.dumps(prediction, indent=2)}")
    else:
        logger.error(f"Failed to predict price: {prediction['error']}")
    
    logger.info("Testing analyze_sentiment...")
    sentiment = analyzer.analyze_sentiment("AAPL")
    if "error" not in sentiment:
        logger.info(f"Successfully analyzed sentiment")
        logger.info(f"Sentiment: {json.dumps(sentiment, indent=2)}")
    else:
        logger.error(f"Failed to analyze sentiment: {sentiment['error']}")
    
    logger.info("Analyzer tests completed")

def run_tests():
    """Run all tests."""
    logger.info("Starting Data Analysis Service tests...")
    
    test_analyzer()
    
    logger.info("All tests completed")

if __name__ == "__main__":
    run_tests() 