import sys
import os
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config, json_dumps

logger = get_logger("data_processor")
config = load_config()

class DataProcessor:
    """Processes financial data."""
    
    def __init__(self):
        self.logger = logger
        self.storage_service_url = f"http://{config['db']['host']}:{config['services']['data_storage']['port']}"
    
    def process_historical_data(self, symbol, data):
        """
        Process historical data.
        
        Args:
            symbol (str): Stock symbol
            data (list): Historical data
        
        Returns:
            dict: Processed data
        """
        try:
            self.logger.info(f"Processing historical data for {symbol} with {len(data)} records")
            
            df = pd.DataFrame(data)
            
            if len(df) >= 5:
                df['ma5'] = df['close'].rolling(window=5).mean()
            if len(df) >= 20:
                df['ma20'] = df['close'].rolling(window=20).mean()
            
            if len(df) > 1:
                df['daily_return'] = df['close'].pct_change()
            
            if len(df) > 1:
                df['volatility'] = df['daily_return'].rolling(window=min(len(df), 20)).std()
            
            if len(df) > 14:
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0)
                loss = -delta.where(delta < 0, 0)
                avg_gain = gain.rolling(window=14).mean()
                avg_loss = loss.rolling(window=14).mean()
                rs = avg_gain / avg_loss
                df['rsi'] = 100 - (100 / (1 + rs))
            
            df = df.fillna(0)
            
            processed_data = []
            for _, row in df.iterrows():
                processed_row = {col: row[col] for col in row.index if not pd.isna(row[col])}
                processed_data.append(processed_row)
            
            self._send_to_storage(symbol, processed_data, "historical")
            
            self.logger.info(f"Successfully processed historical data for {symbol}")
            return processed_data
        except Exception as e:
            self.logger.error(f"Error processing historical data for {symbol}: {e}")
            return []
    
    def process_realtime_data(self, data):
        """
        Process real-time data.
        
        Args:
            data (dict): Real-time data
        
        Returns:
            dict: Processed data
        """
        try:
            symbol = data.get("symbol")
            self.logger.info(f"Processing real-time data for {symbol}")
            
            if "timestamp" not in data:
                data["timestamp"] = datetime.now().isoformat()
            
            if "price" in data and "market_cap" in data and data["price"] and data["market_cap"]:
                data["shares_outstanding"] = data["market_cap"] / data["price"]
            
            data["sentiment"] = "neutral"
            if "change_percent" in data and data["change_percent"]:
                if data["change_percent"] > 2:
                    data["sentiment"] = "very_positive"
                elif data["change_percent"] > 0.5:
                    data["sentiment"] = "positive"
                elif data["change_percent"] < -2:
                    data["sentiment"] = "very_negative"
                elif data["change_percent"] < -0.5:
                    data["sentiment"] = "negative"
            
            self._send_to_storage(symbol, data, "realtime")
            
            self.logger.info(f"Successfully processed real-time data for {symbol}")
            return data
        except Exception as e:
            self.logger.error(f"Error processing real-time data: {e}")
            return {}
    
    def _send_to_storage(self, symbol, data, data_type):
        """
        Send processed data to storage service.
        
        Args:
            symbol (str): Stock symbol
            data (dict or list): Processed data
            data_type (str): Type of data (historical or realtime)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            url = f"{self.storage_service_url}/api/v1/{data_type}"
            payload = {
                "symbol": symbol,
                "data": data
            }
            
            
            self.logger.info(f"Would send {data_type} data for {symbol} to storage service")
            
           
            
            return True
        except Exception as e:
            self.logger.error(f"Error sending data to storage service: {e}")
            return False

if __name__ == "__main__":
    import json
    
    processor = DataProcessor()
    
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
    print(f"Processed historical data: {json.dumps(processed_historical, indent=2)}")
    
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
    print(f"Processed real-time data: {json.dumps(processed_realtime, indent=2)}") 