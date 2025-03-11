import sys
import os
import json
from flask import Flask, request, jsonify

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config

from repository import Repository

app = Flask(__name__)

logger = get_logger("data_storage_api")
config = load_config()

repository = Repository()

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "ok"})

@app.route("/api/v1/historical", methods=["POST"])
def store_historical_data():
    """
    Store historical data.
    
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
                    "volume": 1000000,
                    "ma5": 152.0,
                    "ma20": 150.0,
                    "daily_return": 0.02,
                    "volatility": 0.015,
                    "rsi": 65.0
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
        
        success = repository.store_historical_data(symbol, historical_data)
        
        if success:
            return jsonify({"status": "success", "message": f"Stored {len(historical_data)} records for {symbol}"})
        else:
            return jsonify({"error": "Failed to store historical data"}), 500
    except Exception as e:
        logger.error(f"Error storing historical data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/realtime", methods=["POST"])
def store_realtime_data():
    """
    Store real-time data.
    
    Request body:
        {
            "symbol": "AAPL",
            "data": {
                "timestamp": "2023-01-01T12:34:56",
                "price": 153.0,
                "change": 3.0,
                "change_percent": 2.0,
                "volume": 1000000,
                "market_cap": 2500000000000,
                "bid": 152.9,
                "ask": 153.1,
                "shares_outstanding": 16339730000,
                "sentiment": "positive"
            }
        }
    """
    try:
        data = request.json
        
        if not data or "symbol" not in data or "data" not in data:
            return jsonify({"error": "Symbol and data are required"}), 400
        
        symbol = data.get("symbol")
        realtime_data = data.get("data", {})
        
        realtime_data["symbol"] = symbol
        
        success = repository.store_realtime_data(realtime_data)
        
        if success:
            return jsonify({"status": "success", "message": f"Stored real-time data for {symbol}"})
        else:
            return jsonify({"error": "Failed to store real-time data"}), 500
    except Exception as e:
        logger.error(f"Error storing real-time data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/historical/<symbol>", methods=["GET"])
def get_historical_data(symbol):
    """
    Get historical data for a symbol.
    
    Query parameters:
        limit (int, optional): Maximum number of records to return (default: 100)
    """
    try:
        limit = request.args.get("limit", 100, type=int)
        
        data = repository.get_historical_data(symbol, limit)
        
        return jsonify({
            "symbol": symbol,
            "data": data
        })
    except Exception as e:
        logger.error(f"Error getting historical data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/realtime/<symbol>", methods=["GET"])
def get_realtime_data(symbol):
    """Get real-time data for a symbol."""
    try:
        data = repository.get_realtime_data(symbol)
        
        if data:
            return jsonify(data)
        else:
            return jsonify({"error": f"No real-time data found for {symbol}"}), 404
    except Exception as e:
        logger.error(f"Error getting real-time data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/stocks", methods=["GET"])
def get_stocks():
    """Get all stocks."""
    try:
      
        return jsonify({
            "status": "success",
            "message": "This endpoint would return a list of all stocks"
        })
    except Exception as e:
        logger.error(f"Error getting stocks: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = config["services"]["data_storage"]["port"]
    app.run(host="0.0.0.0", port=port, debug=True) 