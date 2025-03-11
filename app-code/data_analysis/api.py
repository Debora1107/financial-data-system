import sys
import os
import json
from flask import Flask, request, jsonify

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config

from analyzer import Analyzer

app = Flask(__name__)

logger = get_logger("data_analysis_api")
config = load_config()

analyzer = Analyzer()

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "ok"})

@app.route("/api/v1/predict/<symbol>", methods=["GET"])
def predict_price(symbol):
    """
    Predict future prices for a symbol.
    
    Query parameters:
        days (int, optional): Number of days to predict (default: 5)
    """
    try:
        days = request.args.get("days", 5, type=int)
        
        prediction = analyzer.predict_price(symbol, days)
        
        return jsonify(prediction)
    except Exception as e:
        logger.error(f"Error predicting price: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/sentiment/<symbol>", methods=["GET"])
def analyze_sentiment(symbol):
    """Analyze sentiment for a symbol."""
    try:
        sentiment = analyzer.analyze_sentiment(symbol)
        
        return jsonify(sentiment)
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/indicators/<symbol>", methods=["GET"])
def get_technical_indicators(symbol):
    """
    Get technical indicators for a symbol.
    
    Query parameters:
        limit (int, optional): Maximum number of records to return (default: 30)
    """
    try:
        limit = request.args.get("limit", 30, type=int)
        
        df = analyzer.get_historical_data(symbol, limit)
        
        if df.empty:
            return jsonify({"error": "No historical data available"}), 404
        
        df = analyzer.calculate_technical_indicators(df)
        
        data = []
        for _, row in df.iterrows():
            data.append({
                "date": row["date"].isoformat(),
                "close": row["close"],
                "ma5": row["ma5"],
                "ma20": row["ma20"],
                "daily_return": row["daily_return"],
                "volatility": row["volatility"],
                "rsi": row["rsi"],
                "bb_middle": row["bb_middle"],
                "bb_upper": row["bb_upper"],
                "bb_lower": row["bb_lower"],
                "macd": row["macd"],
                "macd_signal": row["macd_signal"],
                "macd_histogram": row["macd_histogram"]
            })
        
        return jsonify({
            "symbol": symbol,
            "data": data
        })
    except Exception as e:
        logger.error(f"Error getting technical indicators: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/summary/<symbol>", methods=["GET"])
def get_summary(symbol):
    """Get a summary of analysis for a symbol."""
    try:
        sentiment = analyzer.analyze_sentiment(symbol)
        
        prediction = analyzer.predict_price(symbol, days=5)
        
        summary = {
            "symbol": symbol,
            "current_price": sentiment.get("price"),
            "change_percent": sentiment.get("change_percent"),
            "sentiment": sentiment.get("sentiment"),
            "signals": sentiment.get("signals"),
            "prediction": {
                "tomorrow": prediction.get("predictions", [])[0] if prediction.get("predictions") else None,
                "five_day": prediction.get("predictions", [])
            }
        }
        
        return jsonify(summary)
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = config["services"]["data_analysis"]["port"]
    app.run(host="0.0.0.0", port=port, debug=True) 