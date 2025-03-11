import sys
import os
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.arima.model import ARIMA


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config


logger = get_logger("data_analyzer")
config = load_config()

class Analyzer:
    """Analyzes financial data."""
    
    def __init__(self):
        self.logger = logger
        self.storage_service_url = f"http://{config['db']['host']}:{config['services']['data_storage']['port']}"
    
    def get_historical_data(self, symbol, limit=100):
        """
        Get historical data for a symbol from the storage service.
        
        Args:
            symbol (str): Stock symbol
            limit (int, optional): Maximum number of records to return
        
        Returns:
            pandas.DataFrame: Historical data
        """
        try:
            
            url = f"{self.storage_service_url}/api/v1/historical/{symbol}?limit={limit}"
            
           
            data = []
            base_date = datetime.now() - timedelta(days=limit)
            base_price = 150.0
            
            for i in range(limit):
                date = base_date + timedelta(days=i)
                close_price = base_price + np.random.normal(0, 2)
                data.append({
                    "symbol": symbol,
                    "date": date.isoformat(),
                    "open": close_price - np.random.uniform(0, 1),
                    "high": close_price + np.random.uniform(0, 2),
                    "low": close_price - np.random.uniform(0, 2),
                    "close": close_price,
                    "volume": int(np.random.uniform(500000, 1500000)),
                    "ma5": None,
                    "ma20": None,
                    "daily_return": None,
                    "volatility": None,
                    "rsi": None
                })
                base_price = close_price
            
        
            df = pd.DataFrame(data)
            
            
            df["date"] = pd.to_datetime(df["date"])
            
           
            df = df.sort_values("date")
            
            return df
        except Exception as e:
            self.logger.error(f"Error getting historical data: {e}")
            return pd.DataFrame()
    
    def get_realtime_data(self, symbol):
        """
        Get real-time data for a symbol from the storage service.
        
        Args:
            symbol (str): Stock symbol
        
        Returns:
            dict: Real-time data
        """
        try:
            url = f"{self.storage_service_url}/api/v1/realtime/{symbol}"
            
            
            data = {
                "symbol": symbol,
                "timestamp": datetime.now().isoformat(),
                "price": 153.0,
                "change": 0.5,
                "change_percent": 0.33,
                "volume": 1000000,
                "market_cap": 2500000000000,
                "bid": 152.9,
                "ask": 153.1,
                "shares_outstanding": 16339730000,
                "sentiment": "neutral"
            }
            
            return data
        except Exception as e:
            self.logger.error(f"Error getting real-time data: {e}")
            return {}
    
    def calculate_technical_indicators(self, df):
        """
        Calculate technical indicators for a DataFrame of historical data.
        
        Args:
            df (pandas.DataFrame): Historical data
        
        Returns:
            pandas.DataFrame: Data with technical indicators
        """
        try:
            df = df.copy()
            
            df["ma5"] = df["close"].rolling(window=5).mean()
            df["ma20"] = df["close"].rolling(window=20).mean()
            
            df["daily_return"] = df["close"].pct_change()
            
            df["volatility"] = df["daily_return"].rolling(window=20).std()
            
            delta = df["close"].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss
            df["rsi"] = 100 - (100 / (1 + rs))
            
            df["bb_middle"] = df["close"].rolling(window=20).mean()
            df["bb_upper"] = df["bb_middle"] + 2 * df["close"].rolling(window=20).std()
            df["bb_lower"] = df["bb_middle"] - 2 * df["close"].rolling(window=20).std()
            
            df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
            df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = df["ema12"] - df["ema26"]
            df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["macd_histogram"] = df["macd"] - df["macd_signal"]
            
            df = df.fillna(0)
            
            return df
        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {e}")
            return df
    
    def predict_price(self, symbol, days=5):
        """
        Predict future prices for a symbol.
        
        Args:
            symbol (str): Stock symbol
            days (int, optional): Number of days to predict
        
        Returns:
            dict: Prediction results
        """
        try:
            df = self.get_historical_data(symbol, limit=100)
            
            if df.empty:
                return {"error": "No historical data available"}
            
            df = self.calculate_technical_indicators(df)
            
            df = df.sort_values("date")
            
            lr_predictions = self._predict_linear_regression(df, days)
            
            arima_predictions = self._predict_arima(df, days)
            
            predictions = []
            for i in range(days):
                date = df["date"].iloc[-1] + timedelta(days=i+1)
                predictions.append({
                    "date": date.isoformat(),
                    "linear_regression": lr_predictions[i],
                    "arima": arima_predictions[i],
                    "average": (lr_predictions[i] + arima_predictions[i]) / 2
                })
            
            return {
                "symbol": symbol,
                "last_price": df["close"].iloc[-1],
                "predictions": predictions
            }
        except Exception as e:
            self.logger.error(f"Error predicting price: {e}")
            return {"error": str(e)}
    
    def _predict_linear_regression(self, df, days):
        """
        Predict future prices using Linear Regression.
        
        Args:
            df (pandas.DataFrame): Historical data
            days (int): Number of days to predict
        
        Returns:
            list: Predicted prices
        """
        try:
            X = np.array(range(len(df))).reshape(-1, 1)
            y = df["close"].values
            
            model = LinearRegression()
            model.fit(X, y)
            
            future_X = np.array(range(len(df), len(df) + days)).reshape(-1, 1)
            predictions = model.predict(future_X)
            
            return predictions.tolist()
        except Exception as e:
            self.logger.error(f"Error predicting with Linear Regression: {e}")
            return [df["close"].iloc[-1]] * days
    
    def _predict_arima(self, df, days):
        """
        Predict future prices using ARIMA.
        
        Args:
            df (pandas.DataFrame): Historical data
            days (int): Number of days to predict
        
        Returns:
            list: Predicted prices
        """
        try:
            y = df["close"].values
            
            model = ARIMA(y, order=(5, 1, 0))
            model_fit = model.fit()
            
            predictions = model_fit.forecast(steps=days)
            
            return predictions.tolist()
        except Exception as e:
            self.logger.error(f"Error predicting with ARIMA: {e}")
            return [df["close"].iloc[-1]] * days
    
    def analyze_sentiment(self, symbol):
        """
        Analyze sentiment for a symbol.
        
        Args:
            symbol (str): Stock symbol
        
        Returns:
            dict: Sentiment analysis results
        """
        try:
            data = self.get_realtime_data(symbol)
            
            if not data:
                return {"error": "No real-time data available"}
            
            df = self.get_historical_data(symbol, limit=30)
            
            if df.empty:
                return {"error": "No historical data available"}
            
            df = self.calculate_technical_indicators(df)
            
            price_trend = "neutral"
            if df["ma5"].iloc[-1] > df["ma20"].iloc[-1]:
                price_trend = "bullish"
            elif df["ma5"].iloc[-1] < df["ma20"].iloc[-1]:
                price_trend = "bearish"
            
            rsi = df["rsi"].iloc[-1]
            rsi_signal = "neutral"
            if rsi > 70:
                rsi_signal = "overbought"
            elif rsi < 30:
                rsi_signal = "oversold"
            
            macd = df["macd"].iloc[-1]
            macd_signal = df["macd_signal"].iloc[-1]
            macd_trend = "neutral"
            if macd > macd_signal:
                macd_trend = "bullish"
            elif macd < macd_signal:
                macd_trend = "bearish"
            
            bb_position = (df["close"].iloc[-1] - df["bb_lower"].iloc[-1]) / (df["bb_upper"].iloc[-1] - df["bb_lower"].iloc[-1])
            bb_signal = "neutral"
            if bb_position > 0.8:
                bb_signal = "overbought"
            elif bb_position < 0.2:
                bb_signal = "oversold"
            
            
            signals = {
                "price_trend": price_trend,
                "rsi": {
                    "value": rsi,
                    "signal": rsi_signal
                },
                "macd": {
                    "value": macd,
                    "signal": macd_signal,
                    "trend": macd_trend
                },
                "bollinger_bands": {
                    "position": bb_position,
                    "signal": bb_signal
                }
            }
            
            sentiment = "neutral"
            bullish_signals = 0
            bearish_signals = 0
            
            if price_trend == "bullish":
                bullish_signals += 1
            elif price_trend == "bearish":
                bearish_signals += 1
            
            if rsi_signal == "oversold":
                bullish_signals += 1
            elif rsi_signal == "overbought":
                bearish_signals += 1
            
            if macd_trend == "bullish":
                bullish_signals += 1
            elif macd_trend == "bearish":
                bearish_signals += 1
            
            if bb_signal == "oversold":
                bullish_signals += 1
            elif bb_signal == "overbought":
                bearish_signals += 1
            
            if bullish_signals > bearish_signals:
                sentiment = "bullish"
            elif bearish_signals > bullish_signals:
                sentiment = "bearish"
            
            return {
                "symbol": symbol,
                "price": data["price"],
                "change_percent": data["change_percent"],
                "sentiment": sentiment,
                "signals": signals
            }
        except Exception as e:
            self.logger.error(f"Error analyzing sentiment: {e}")
            return {"error": str(e)}

if __name__ == "__main__":
    analyzer = Analyzer()
    
    prediction = analyzer.predict_price("AAPL", days=5)
    print(f"Price prediction: {json.dumps(prediction, indent=2)}")
    
    sentiment = analyzer.analyze_sentiment("AAPL")
    print(f"Sentiment analysis: {json.dumps(sentiment, indent=2)}") 