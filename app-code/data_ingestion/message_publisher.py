import sys
import os
import pika
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config, json_dumps

logger = get_logger("message_publisher")
config = load_config()

class MessagePublisher:
    """Publishes messages to RabbitMQ."""
    
    def __init__(self):
        self.logger = logger
        self.rabbitmq_config = config["rabbitmq"]
        self.connection = None
        self.channel = None
        
    def connect(self):
        """Connect to RabbitMQ."""
        try:
       
            self.logger.info("Connecting to RabbitMQ...")
            
          
            credentials = pika.PlainCredentials(
                self.rabbitmq_config["user"],
                self.rabbitmq_config["password"]
            )
            parameters = pika.ConnectionParameters(
                host=self.rabbitmq_config["host"],
                port=self.rabbitmq_config["port"],
                credentials=credentials
            )
            
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
           
            self.channel.exchange_declare(
                exchange="financial_data",
                exchange_type="topic",
                durable=True
            )
            
            self.logger.info("Successfully connected to RabbitMQ")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to RabbitMQ: {e}")
            return False
    
    def publish_message(self, routing_key, message):
        """
        Publish a message to RabbitMQ.
        
        Args:
            routing_key (str): Routing key for the message
            message (dict): Message to publish
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return False
            
            message_json = json_dumps(message)
            
            self.channel.basic_publish(
                exchange="financial_data",
                routing_key=routing_key,
                body=message_json,
                properties=pika.BasicProperties(
                    delivery_mode=2,  
                    content_type="application/json"
                )
            )
            
            self.logger.info(f"Published message with routing key: {routing_key}")
            return True
        except Exception as e:
            self.logger.error(f"Error publishing message: {e}")
            return False
    
    def publish_historical_data(self, symbol, data):
        """
        Publish historical data for a symbol.
        
        Args:
            symbol (str): Stock symbol
            data (list): Historical data
        
        Returns:
            bool: True if successful, False otherwise
        """
        routing_key = f"financial_data.historical.{symbol}"
        message = {
            "type": "historical",
            "symbol": symbol,
            "data": data
        }
        return self.publish_message(routing_key, message)
    
    def publish_real_time_data(self, data):
        """
        Publish real-time data.
        
        Args:
            data (dict): Real-time data
        
        Returns:
            bool: True if successful, False otherwise
        """
        symbol = data.get("symbol")
        routing_key = f"financial_data.realtime.{symbol}"
        message = {
            "type": "realtime",
            "data": data
        }
        return self.publish_message(routing_key, message)
    
    def close(self):
        """Close the connection to RabbitMQ."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("Closed connection to RabbitMQ")

if __name__ == "__main__":
    from data_fetcher import YahooFinanceFetcher
    
    fetcher = YahooFinanceFetcher()
    publisher = MessagePublisher()
    
    if publisher.connect():
        apple_data = fetcher.fetch_historical_data("AAPL", period="5d")
        publisher.publish_historical_data("AAPL", apple_data)
        
        msft_data = fetcher.fetch_real_time_data("MSFT")
        publisher.publish_real_time_data(msft_data)
        
        publisher.close()
    else:
        print("Failed to connect to RabbitMQ") 