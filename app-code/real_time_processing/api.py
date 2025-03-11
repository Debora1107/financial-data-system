import sys
import os
import json
from flask import Flask, request, jsonify

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config

from data_processor import DataProcessor
from message_consumer import MessageConsumer

app = Flask(__name__)

logger = get_logger("real_time_processing_api")
config = load_config()

processor = DataProcessor()
consumer = MessageConsumer(processor)

consumer.start_consuming_in_thread()

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "ok"})

@app.route("/api/v1/process/historical", methods=["POST"])
def process_historical_data():
    """
    Process historical data.
    
    Request body:
        {
            "symbol": "AAPL",
            "data": [
                {
                    "date": "2023-01-01T00:00:00",
                    "open": 150.0,
                    "high": 155.0,
                    "low": 149.0,
                    "close": 153.0,
                    "volume": 1000000
                },
                ...
            ]
        }
    """
    try:
        data = request.json
        
        if not data or "symbol" not in data or "data" not in data:
            return jsonify({"error": "Symbol and data are required"}), 400
        
        symbol = data.get("symbol")
        historical_data = data.get("data", [])
        
        processed_data = processor.process_historical_data(symbol, historical_data)
        
        return jsonify({
            "symbol": symbol,
            "processed_data": processed_data
        })
    except Exception as e:
        logger.error(f"Error processing historical data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/process/realtime", methods=["POST"])
def process_realtime_data():
    """
    Process real-time data.
    
    Request body:
        {
            "symbol": "AAPL",
            "timestamp": "2023-01-01T12:34:56",
            "price": 153.0,
            "change": 3.0,
            "change_percent": 2.0,
            "volume": 1000000,
            "market_cap": 2500000000000
        }
    """
    try:
        data = request.json
        
        if not data or "symbol" not in data:
            return jsonify({"error": "Symbol is required"}), 400
        
        processed_data = processor.process_realtime_data(data)
        
        return jsonify(processed_data)
    except Exception as e:
        logger.error(f"Error processing real-time data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/consumer/status", methods=["GET"])
def consumer_status():
    """Get the status of the message consumer."""
    try:
        status = "running" if consumer.consumer_thread and consumer.consumer_thread.is_alive() else "stopped"
        return jsonify({"status": status})
    except Exception as e:
        logger.error(f"Error getting consumer status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/consumer/start", methods=["POST"])
def start_consumer():
    """Start the message consumer."""
    try:
        if consumer.consumer_thread and consumer.consumer_thread.is_alive():
            return jsonify({"status": "already_running"})
        
        consumer.start_consuming_in_thread()
        return jsonify({"status": "started"})
    except Exception as e:
        logger.error(f"Error starting consumer: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/consumer/stop", methods=["POST"])
def stop_consumer():
    """Stop the message consumer."""
    try:
        consumer.stop_consuming()
        return jsonify({"status": "stopped"})
    except Exception as e:
        logger.error(f"Error stopping consumer: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = config["services"]["real_time_processing"]["port"]
    app.run(host="0.0.0.0", port=port, debug=True) 