import sys
import os
import time
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config

logger = get_logger("data_fetcher")
config = load_config()

class YahooFinanceFetcher:
    """Fetches financial data from Yahoo Finance API."""
    
    def __init__(self):
        self.logger = logger
        
    def fetch_historical_data(self, symbol, period="1mo", interval="1d"):
        """
        Fetch historical data for a given symbol.
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
            period (str): Period to fetch data for (e.g., '1d', '1mo', '1y')
            interval (str): Data interval (e.g., '1m', '1h', '1d')
            
        Returns:
            pandas.DataFrame: Historical data
        """
        try:
            self.logger.info(f"Fetching historical data for {symbol} with period={period}, interval={interval}")
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            
            data = data.reset_index()
            
            result = []
            for _, row in data.iterrows():
                result.append({
                    "symbol": symbol,
                    "date": row["Date"].isoformat(),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                })
            
            self.logger.info(f"Successfully fetched {len(result)} records for {symbol}")
            return result
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {e}")
            return []
    
    def fetch_real_time_data(self, symbol):
        """
        Fetch real-time data for a given symbol.
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
            
        Returns:
            dict: Real-time data
        """
        try:
            self.logger.info(f"Fetching real-time data for {symbol}")
            ticker = yf.Ticker(symbol)
            data = ticker.info
            
            result = {
                "symbol": symbol,
                "timestamp": datetime.now().isoformat(),
                "price": data.get("regularMarketPrice", None),
                "change": data.get("regularMarketChange", None),
                "change_percent": data.get("regularMarketChangePercent", None),
                "volume": data.get("regularMarketVolume", None),
                "market_cap": data.get("marketCap", None),
                "bid": data.get("bid", None),
                "ask": data.get("ask", None),
            }
            
            self.logger.info(f"Successfully fetched real-time data for {symbol}")
            return result
        except Exception as e:
            self.logger.error(f"Error fetching real-time data for {symbol}: {e}")
            return {}
    
    def fetch_multiple_symbols(self, symbols, period="1mo", interval="1d"):
        """
        Fetch historical data for multiple symbols.
        
        Args:
            symbols (list): List of stock symbols
            period (str): Period to fetch data for
            interval (str): Data interval
            
        Returns:
            dict: Historical data for each symbol
        """
        result = {}
        for symbol in symbols:
            result[symbol] = self.fetch_historical_data(symbol, period, interval)
            
            time.sleep(0.5)
        return result

# Example usage
if __name__ == "__main__":
    fetcher = YahooFinanceFetcher()
    
 
    apple_data = fetcher.fetch_historical_data("AAPL", period="5d", interval="1d")
    print(f"Apple historical data: {apple_data}")
    
  
    msft_data = fetcher.fetch_real_time_data("MSFT")
    print(f"Microsoft real-time data: {msft_data}")
    
   
    multi_data = fetcher.fetch_multiple_symbols(["AAPL", "MSFT", "GOOG"], period="1d")
    print(f"Multi-symbol data: {multi_data}") 