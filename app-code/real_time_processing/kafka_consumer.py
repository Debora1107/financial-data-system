from kafka import KafkaConsumer
import logging
import os
import json
from datetime import datetime
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaStreamProcessor:
    def __init__(self):
        try:
            logger.info("Initializing Kafka consumer...")
            
            self.processed_dir = os.path.join(os.getcwd(), "data", "processed")
            os.makedirs(self.processed_dir, exist_ok=True)
            
            self.consumer = KafkaConsumer(
                'financial_data',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='financial_data_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            logger.info("KafkaStreamProcessor initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing KafkaStreamProcessor: {e}")
            raise

    def process_stream(self):
        try:
            logger.info("Starting Kafka stream processing...")
            
            for message in self.consumer:
                try:
                    data = message.value
                    
                    logger.info(f"Received data for symbol: {data.get('symbol', 'unknown')}")
                    
                    self._process_data(data)
                    
                    self._save_to_file(data)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
        except KeyboardInterrupt:
            logger.info("Stopping Kafka stream processing...")
        except Exception as e:
            logger.error(f"Error processing stream: {e}")
        finally:
            self.cleanup()

    def _process_data(self, data):
        """Process the data from Kafka"""
        try:
            data['processing_timestamp'] = datetime.now().isoformat()
            
            if 'price' in data and 'volume' in data:
                data['value'] = data['price'] * data['volume']
                
            return data
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return data

    def _save_to_file(self, data):
        """Save processed data to file"""
        try:
            symbol = data.get('symbol', 'unknown')
            symbol_dir = os.path.join(self.processed_dir, symbol)
            os.makedirs(symbol_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{symbol}_{timestamp}.json"
            filepath = os.path.join(symbol_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Saved data to {filepath}")
        except Exception as e:
            logger.error(f"Error saving data to file: {e}")

    def cleanup(self):
        """Clean up resources"""
        try:
            logger.info("Starting cleanup process...")
            
            if hasattr(self, 'consumer'):
                logger.info("Closing Kafka consumer...")
                self.consumer.close()
            
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    try:
        processor = KafkaStreamProcessor()
        processor.process_stream()
    except Exception as e:
        logger.error(f"Error: {e}")