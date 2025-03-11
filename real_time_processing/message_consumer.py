import sys
import os
import json
import pika
import threading
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config, json_loads

from data_processor import DataProcessor

logger = get_logger("message_consumer")
config = load_config()

class MessageConsumer:
    """Consumes messages from RabbitMQ."""
    
    def __init__(self, processor=None):
        self.logger = logger
        self.rabbitmq_config = config["rabbitmq"]
        self.connection = None
        self.channel = None
        self.processor = processor or DataProcessor()
        self.should_stop = False
        self.consumer_thread = None
    
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
            
            self.channel.queue_declare(
                queue="historical_data",
                durable=True
            )
            self.channel.queue_declare(
                queue="realtime_data",
                durable=True
            )
            
            self.channel.queue_bind(
                exchange="financial_data",
                queue="historical_data",
                routing_key="financial_data.historical.*"
            )
            self.channel.queue_bind(
                exchange="financial_data",
                queue="realtime_data",
                routing_key="financial_data.realtime.*"
            )
            
            self.logger.info("Successfully connected to RabbitMQ")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to RabbitMQ: {e}")
            return False
    
    def process_historical_data(self, ch, method, properties, body):
        """
        Process historical data message.
        
        Args:
            ch: Channel
            method: Method
            properties: Properties
            body: Message body
        """
        try:
            message = json_loads(body)
            
            symbol = message.get("symbol")
            data = message.get("data", [])
            
            self.logger.info(f"Received historical data for {symbol} with {len(data)} records")
            
            processed_data = self.processor.process_historical_data(symbol, data)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            self.logger.info(f"Processed historical data for {symbol}")
        except Exception as e:
            self.logger.error(f"Error processing historical data: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def process_realtime_data(self, ch, method, properties, body):
        """
        Process real-time data message.
        
        Args:
            ch: Channel
            method: Method
            properties: Properties
            body: Message body
        """
        try:
            message = json_loads(body)
            
            data = message.get("data", {})
            symbol = data.get("symbol")
            
            self.logger.info(f"Received real-time data for {symbol}")
            
            processed_data = self.processor.process_realtime_data(data)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            self.logger.info(f"Processed real-time data for {symbol}")
        except Exception as e:
            self.logger.error(f"Error processing real-time data: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def start_consuming(self):
        """Start consuming messages."""
        try:
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return False
            
            self.channel.basic_consume(
                queue="historical_data",
                on_message_callback=self.process_historical_data
            )
            self.channel.basic_consume(
                queue="realtime_data",
                on_message_callback=self.process_realtime_data
            )
            
            self.logger.info("Started consuming messages")
            
            self.channel.start_consuming()
            
            return True
        except Exception as e:
            self.logger.error(f"Error starting consumers: {e}")
            return False
    
    def start_consuming_in_thread(self):
        """Start consuming messages in a separate thread."""
        self.should_stop = False
        self.consumer_thread = threading.Thread(target=self._consume_thread)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        self.logger.info("Started consuming messages in a separate thread")
    
    def _consume_thread(self):
        """Consume messages in a separate thread."""
        while not self.should_stop:
            try:
                self.start_consuming()
            except Exception as e:
                self.logger.error(f"Error in consumer thread: {e}")
                time.sleep(5)   
    
    def stop_consuming(self):
        """Stop consuming messages."""
        self.should_stop = True
        if self.connection and not self.connection.is_closed:
            self.channel.stop_consuming()
            self.connection.close()
            self.logger.info("Stopped consuming messages")
    
    def close(self):
        """Close the connection to RabbitMQ."""
        self.stop_consuming()

if __name__ == "__main__":
    consumer = MessageConsumer()
    
    consumer.start_consuming_in_thread()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        consumer.close()
        print("Consumer stopped") 