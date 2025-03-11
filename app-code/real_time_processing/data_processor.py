import sys
import os
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config, json_dumps

logger = get_logger("data_processor")
config = load_config()

class DataProcessor:
    """Processes financial data."""
    
    def __init__(self):
        self.logger = logger
        self.storage_service_url = f"http://{config['db']['host']}:{config['services']['data_storage']['port']}"
        

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            self.logger.info("Successfully connected to Kafka")
        except Exception as e:
            self.logger.error(f"Error connecting to Kafka: {e}")
            self.producer = None
    
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
                
                if self.producer:
                    try:
                        self.producer.send('financial_data', processed_row)
                        self.producer.flush()
                    except Exception as e:
                        self.logger.error(f"Error sending data to Kafka: {e}")
            
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
            
            if self.producer:
                try:
                    self.producer.send('financial_data', data)
                    self.producer.flush()
                except Exception as e:
                    self.logger.error(f"Error sending real-time data to Kafka: {e}")
            
            self._send_to_storage(symbol, data, "realtime")
            
            self.logger.info(f"Successfully processed real-time data for {symbol}")
            return data
        except Exception as e:
            self.logger.error(f"Error processing real-time data: {e}")
            return {}
    
    def _send_to_storage(self, symbol, data, data_type):
        """
        Send data to storage service.
        
        Args:
            symbol (str): Stock symbol
            data (dict/list): Data to store
            data_type (str): Type of data (historical/realtime)
        """
        try:
            url = f"{self.storage_service_url}/api/v1/{data_type}/{symbol}"
            response = requests.post(url, json=data)
            
            if response.status_code != 200:
                self.logger.error(f"Error storing {data_type} data: {response.text}")
        except Exception as e:
            self.logger.error(f"Error sending data to storage: {e}")
    
    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            self.producer.close()
            self.logger.info("Closed Kafka producer connection")