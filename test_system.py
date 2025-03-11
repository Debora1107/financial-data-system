import sys
import os
import json
import time
import requests
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_logger, load_config

logger = get_logger("system_test")
config = load_config()

INGESTION_URL = f"http://localhost:{config['services']['data_ingestion']['port']}"
PROCESSING_URL = f"http://localhost:{config['services']['real_time_processing']['port']}"
STORAGE_URL = f"http://localhost:{config['services']['data_storage']['port']}"
ANALYSIS_URL = f"http://localhost:{config['services']['data_analysis']['port']}"
VISUALIZATION_URL = f"http://localhost:{config['services']['data_visualization']['port']}"

def test_health_endpoints():
    """Test health endpoints of all services."""
    logger.info("Testing health endpoints...")
    
    services = {
        "Data Ingestion": f"{INGESTION_URL}/health",
        "Real-Time Processing": f"{PROCESSING_URL}/health",
        "Data Storage": f"{STORAGE_URL}/health",
        "Data Analysis": f"{ANALYSIS_URL}/health"
    }
    
    all_healthy = True
    
    for name, url in services.items():
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200 and response.json().get("status") == "ok":
                logger.info(f"{name} service is healthy")
            else:
                logger.error(f"{name} service is not healthy: {response.text}")
                all_healthy = False
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to {name} service: {e}")
            all_healthy = False
    
    return all_healthy

def test_data_flow():
    """Test the data flow through the system."""
    logger.info("Testing data flow...")
    
    logger.info("Step 1: Fetching data from Yahoo Finance...")
    symbol = "AAPL"
    
    try:
        response = requests.get(f"{INGESTION_URL}/api/v1/historical?symbol={symbol}&period=5d&publish=true")
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched historical data for {symbol}: {len(data['data'])} records")
        else:
            logger.error(f"Error fetching historical data: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Data Ingestion service: {e}")
        return False
    
    logger.info("Step 2: Processing data...")
    
    try:
        response = requests.post(
            f"{PROCESSING_URL}/api/v1/process/historical",
            json={"symbol": symbol, "data": data["data"]}
        )
        if response.status_code == 200:
            processed_data = response.json()
            logger.info(f"Successfully processed historical data for {symbol}")
        else:
            logger.error(f"Error processing historical data: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Real-Time Processing service: {e}")
        return False
    
    logger.info("Step 3: Storing data...")
    
    try:
        response = requests.post(
            f"{STORAGE_URL}/api/v1/historical",
            json={"symbol": symbol, "data": processed_data["processed_data"]}
        )
        if response.status_code == 200:
            logger.info(f"Successfully stored historical data for {symbol}")
        else:
            logger.error(f"Error storing historical data: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Data Storage service: {e}")
        return False
    
    logger.info("Step 4: Analyzing data...")
    
    try:
        response = requests.get(f"{ANALYSIS_URL}/api/v1/predict/{symbol}")
        if response.status_code == 200:
            prediction = response.json()
            logger.info(f"Successfully predicted price for {symbol}")
        else:
            logger.error(f"Error predicting price: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Data Analysis service: {e}")
        return False
    
    logger.info("Step 5: Checking visualization service...")
    
    try:
        response = requests.get(VISUALIZATION_URL)
        if response.status_code == 200:
            logger.info("Successfully connected to Data Visualization service")
        else:
            logger.error(f"Error connecting to Data Visualization service: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Data Visualization service: {e}")
        return False
    
    logger.info("Data flow test completed successfully")
    return True

def run_tests():
    """Run all tests."""
    logger.info("Starting system tests...")
    
    if not test_health_endpoints():
        logger.error("Health endpoints test failed")
        return False
    
    if not test_data_flow():
        logger.error("Data flow test failed")
        return False
    
    logger.info("All system tests completed successfully")
    return True

if __name__ == "__main__":
    run_tests() 