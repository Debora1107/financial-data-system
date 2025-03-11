import sys
import os
import json
import requests
from datetime import datetime, timedelta
import random
import numpy as np

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_logger, load_config

# Initialize logger and config
logger = get_logger("load_sample_data")
config = load_config()

# Define service URLs
INGESTION_URL = f"http://localhost:{config['services']['data_ingestion']['port']}"
STORAGE_URL = f"http://localhost:{config['services']['data_storage']['port']}"

def load_sample_data():
    """Load sample data into the system."""
    logger.info("Loading sample data...")
    
    # Define symbols
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]
    
    try:
        for symbol in symbols:
            logger.info(f"Generating sample data for {symbol}...")
            
            # Generate and store historical data
            data = generate_sample_data(symbol, 30)
            logger.info(f"Generated {len(data)} historical records for {symbol}")
            
            response = requests.post(
                f"{STORAGE_URL}/api/v1/historical",
                json={"symbol": symbol, "data": data},
                timeout=5  # 5 seconds timeout
            )
            if response.status_code == 200:
                logger.info(f"Successfully stored historical data for {symbol}")
            else:
                logger.error(f"Error storing historical data for {symbol}: {response.text}")
            
            # Generate and store real-time data
            realtime_data = generate_sample_realtime_data(symbol)
            logger.info(f"Generated real-time data for {symbol}")
            
            response = requests.post(
                f"{STORAGE_URL}/api/v1/realtime",
                json={"symbol": symbol, "data": realtime_data},
                timeout=5  # 5 seconds timeout
            )
            if response.status_code == 200:
                logger.info(f"Successfully stored real-time data for {symbol}")
            else:
                logger.error(f"Error storing real-time data for {symbol}: {response.text}")
        
        logger.info("Sample data loaded successfully")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to services: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def generate_sample_data(symbol, days):
    """Generate sample historical data."""
    data = []
    base_date = datetime.now() - timedelta(days=days)
    
    # Define base prices for different symbols
    base_prices = {
        "AAPL": 150.0,
        "MSFT": 300.0,
        "GOOG": 120.0,
        "AMZN": 130.0,
        "META": 300.0
    }
    
    base_price = base_prices.get(symbol, 100.0)
    
    for i in range(days):
        date = base_date + timedelta(days=i)
        
        # Generate price with some randomness and trend
        trend = 0.0002 * i  # Slight upward trend
        noise = np.random.normal(0, 0.01)
        change = trend + noise
        
        # Calculate prices
        close_price = base_price * (1 + change)
        open_price = close_price * (1 + np.random.normal(0, 0.005))
        high_price = max(close_price, open_price) * (1 + abs(np.random.normal(0, 0.005)))
        low_price = min(close_price, open_price) * (1 - abs(np.random.normal(0, 0.005)))
        
        # Calculate volume
        volume = int(np.random.uniform(1000000, 5000000))
        
        # Add data point
        data.append({
            "date": date.isoformat(),
            "open": float(open_price),
            "high": float(high_price),
            "low": float(low_price),
            "close": float(close_price),
            "volume": volume
        })
        
        # Update base price for next day
        base_price = close_price
    
    return data

def generate_sample_realtime_data(symbol):
    """Generate sample real-time data."""
    # Define base prices for different symbols
    base_prices = {
        "AAPL": 150.0,
        "MSFT": 300.0,
        "GOOG": 120.0,
        "AMZN": 130.0,
        "META": 300.0
    }
    
    price = base_prices.get(symbol, 100.0) * (1 + np.random.normal(0, 0.01))
    change = price - base_prices.get(symbol, 100.0)
    change_percent = (change / base_prices.get(symbol, 100.0)) * 100
    
    # Define market caps
    market_caps = {
        "AAPL": 2500000000000,
        "MSFT": 2400000000000,
        "GOOG": 1500000000000,
        "AMZN": 1300000000000,
        "META": 800000000000
    }
    
    market_cap = market_caps.get(symbol, 500000000000)
    
    # Calculate shares outstanding
    shares_outstanding = market_cap / price
    
    return {
        "timestamp": datetime.now().isoformat(),
        "price": float(price),
        "change": float(change),
        "change_percent": float(change_percent),
        "volume": int(np.random.uniform(1000000, 5000000)),
        "market_cap": float(market_cap),
        "bid": float(price * 0.999),
        "ask": float(price * 1.001),
        "shares_outstanding": float(shares_outstanding)
    }

if __name__ == "__main__":
    load_sample_data() 