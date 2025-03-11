import sys
import os
import json
from flask import Flask, request, jsonify

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config

from data_fetcher import YahooFinanceFetcher
from message_publisher import MessagePublisher

app = Flask(__name__)

logger = get_logger("data_ingestion_api")
config = load_config()

fetcher = YahooFinanceFetcher()
publisher = MessagePublisher()

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "ok"})

@app.route("/api/v1/historical", methods=["GET"])
def get_historical_data():
    """
    Get historical data for a symbol.
    
    Query parameters:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
        period (str, optional): Period to fetch data for (default: '1mo')
        interval (str, optional): Data interval (default: '1d')
        publish (bool, optional): Whether to publish data to RabbitMQ (default: False)
    """
    try:
        symbol = request.args.get("symbol")
        period = request.args.get("period", "1mo")
        interval = request.args.get("interval", "1d")
        publish = request.args.get("publish", "false").lower() == "true"
        
        if not symbol:
            return jsonify({"error": "Symbol is required"}), 400
        
        data = fetcher.fetch_historical_data(symbol, period, interval)
        
        if publish:
            publisher.publish_historical_data(symbol, data)
        
        return jsonify({"symbol": symbol, "data": data})
    except Exception as e:
        logger.error(f"Error getting historical data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/realtime", methods=["GET"])
def get_real_time_data():
    """
    Get real-time data for a symbol.
    
    Query parameters:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
        publish (bool, optional): Whether to publish data to RabbitMQ (default: False)
    """
    try:
        symbol = request.args.get("symbol")
        publish = request.args.get("publish", "false").lower() == "true"
        
        if not symbol:
            return jsonify({"error": "Symbol is required"}), 400
        
        data = fetcher.fetch_real_time_data(symbol)
        
        if publish:
            publisher.publish_real_time_data(data)
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting real-time data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/batch", methods=["POST"])
def batch_fetch():
    """
    Fetch data for multiple symbols.
    
    Request body:
        {
            "symbols": ["AAPL", "MSFT", "GOOG"],
            "period": "1mo",
            "interval": "1d",
            "publish": false
        }
    """
    try:
        data = request.json
        
        if not data or "symbols" not in data:
            return jsonify({"error": "Symbols are required"}), 400
        
        symbols = data.get("symbols", [])
        period = data.get("period", "1mo")
        interval = data.get("interval", "1d")
        publish = data.get("publish", False)
        
        result = fetcher.fetch_multiple_symbols(symbols, period, interval)
        
        if publish:
            for symbol, symbol_data in result.items():
                publisher.publish_historical_data(symbol, symbol_data)
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in batch fetch: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/schedule", methods=["POST"])
def schedule_fetch():
    """
    Schedule periodic data fetching.
    
    Request body:
        {
            "symbol": "AAPL",
            "interval": "1h",
            "duration": "1d"
        }
    
    Note: In a real implementation, this would use a task scheduler like Celery.
    For this school project, we'll just return a success message.
    """
    try:
        data = request.json
        
        if not data or "symbol" not in data:
            return jsonify({"error": "Symbol is required"}), 400
        
     
        symbol = data.get("symbol")
        interval = data.get("interval", "1h")
        duration = data.get("duration", "1d")
        
        
        
        return jsonify({
            "status": "scheduled",
            "symbol": symbol,
            "interval": interval,
            "duration": duration
        })
    except Exception as e:
        logger.error(f"Error scheduling fetch: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
 
    publisher.connect()
    
    
    port = config["services"]["data_ingestion"]["port"]
    app.run(host="0.0.0.0", port=port, debug=True) 