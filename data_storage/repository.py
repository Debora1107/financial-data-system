import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, get_db_url


from models import Base, Stock, HistoricalData, RealtimeData


logger = get_logger("data_storage_repository")

class Repository:
    """Repository for data storage."""
    
    def __init__(self):
        self.logger = logger
        self.engine = None
        self.Session = None
        self.init_db()
    
    def init_db(self):
        """Initialize the database."""
        try:
            self.engine = create_engine(get_db_url())
            
            Base.metadata.create_all(self.engine)
            
            self.Session = sessionmaker(bind=self.engine)
            
            self.logger.info("Database initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            return False
    
    def get_or_create_stock(self, symbol, name=None, sector=None, industry=None):
        """
        Get or create a stock.
        
        Args:
            symbol (str): Stock symbol
            name (str, optional): Stock name
            sector (str, optional): Stock sector
            industry (str, optional): Stock industry
        
        Returns:
            Stock: Stock object
        """
        try:
            session = self.Session()
            
            stock = session.query(Stock).filter_by(symbol=symbol).first()
            
            if not stock:
                stock = Stock(
                    symbol=symbol,
                    name=name,
                    sector=sector,
                    industry=industry
                )
                session.add(stock)
                session.commit()
                self.logger.info(f"Created new stock: {symbol}")
            
            session.close()
            
            return stock
        except Exception as e:
            self.logger.error(f"Error getting or creating stock: {e}")
            if session:
                session.rollback()
                session.close()
            return None
    
    def store_historical_data(self, symbol, data):
        """
        Store historical data.
        
        Args:
            symbol (str): Stock symbol
            data (list): Historical data
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            session = self.Session()
            
            stock = self.get_or_create_stock(symbol)
            if not stock:
                return False
            
            for item in data:
                date = datetime.fromisoformat(item["date"]) if isinstance(item["date"], str) else item["date"]
                
                existing = session.query(HistoricalData).filter_by(
                    stock_id=stock.id,
                    date=date
                ).first()
                
                if existing:
                    existing.open = item["open"]
                    existing.high = item["high"]
                    existing.low = item["low"]
                    existing.close = item["close"]
                    existing.volume = item["volume"]
                    
                    if "ma5" in item:
                        existing.ma5 = item["ma5"]
                    if "ma20" in item:
                        existing.ma20 = item["ma20"]
                    if "daily_return" in item:
                        existing.daily_return = item["daily_return"]
                    if "volatility" in item:
                        existing.volatility = item["volatility"]
                    if "rsi" in item:
                        existing.rsi = item["rsi"]
                else:
                    historical_data = HistoricalData(
                        stock_id=stock.id,
                        date=date,
                        open=item["open"],
                        high=item["high"],
                        low=item["low"],
                        close=item["close"],
                        volume=item["volume"],
                        ma5=item.get("ma5"),
                        ma20=item.get("ma20"),
                        daily_return=item.get("daily_return"),
                        volatility=item.get("volatility"),
                        rsi=item.get("rsi")
                    )
                    session.add(historical_data)
            
            session.commit()
            
            session.close()
            
            self.logger.info(f"Stored {len(data)} historical data records for {symbol}")
            return True
        except Exception as e:
            self.logger.error(f"Error storing historical data: {e}")
            if session:
                session.rollback()
                session.close()
            return False
    
    def store_realtime_data(self, data):
        """
        Store real-time data.
        
        Args:
            data (dict): Real-time data
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            session = self.Session()
            
            symbol = data.get("symbol")
            if not symbol:
                self.logger.error("Symbol is required for real-time data")
                return False
            
            stock = self.get_or_create_stock(symbol)
            if not stock:
                return False
            
            timestamp = datetime.fromisoformat(data["timestamp"]) if isinstance(data["timestamp"], str) else data["timestamp"]
            
            realtime_data = RealtimeData(
                stock_id=stock.id,
                timestamp=timestamp,
                price=data["price"],
                change=data.get("change"),
                change_percent=data.get("change_percent"),
                volume=data.get("volume"),
                market_cap=data.get("market_cap"),
                bid=data.get("bid"),
                ask=data.get("ask"),
                shares_outstanding=data.get("shares_outstanding"),
                sentiment=data.get("sentiment")
            )
            session.add(realtime_data)
            
            session.commit()
            
            session.close()
            
            self.logger.info(f"Stored real-time data for {symbol}")
            return True
        except Exception as e:
            self.logger.error(f"Error storing real-time data: {e}")
            if session:
                session.rollback()
                session.close()
            return False
    
    def get_historical_data(self, symbol, limit=100):
        """
        Get historical data for a symbol.
        
        Args:
            symbol (str): Stock symbol
            limit (int, optional): Maximum number of records to return
        
        Returns:
            list: Historical data
        """
        try:
            session = self.Session()
            
            stock = session.query(Stock).filter_by(symbol=symbol).first()
            if not stock:
                self.logger.error(f"Stock not found: {symbol}")
                return []
            
            query = session.query(HistoricalData).filter_by(stock_id=stock.id).order_by(desc(HistoricalData.date)).limit(limit)
            data = query.all()
            
            result = []
            for item in data:
                result.append({
                    "symbol": symbol,
                    "date": item.date.isoformat(),
                    "open": item.open,
                    "high": item.high,
                    "low": item.low,
                    "close": item.close,
                    "volume": item.volume,
                    "ma5": item.ma5,
                    "ma20": item.ma20,
                    "daily_return": item.daily_return,
                    "volatility": item.volatility,
                    "rsi": item.rsi
                })
            
            session.close()
            
            self.logger.info(f"Retrieved {len(result)} historical data records for {symbol}")
            return result
        except Exception as e:
            self.logger.error(f"Error getting historical data: {e}")
            if session:
                session.close()
            return []
    
    def get_realtime_data(self, symbol):
        """
        Get the latest real-time data for a symbol.
        
        Args:
            symbol (str): Stock symbol
        
        Returns:
            dict: Real-time data
        """
        try:
            session = self.Session()
            
            stock = session.query(Stock).filter_by(symbol=symbol).first()
            if not stock:
                self.logger.error(f"Stock not found: {symbol}")
                return {}
            
            data = session.query(RealtimeData).filter_by(stock_id=stock.id).order_by(desc(RealtimeData.timestamp)).first()
            if not data:
                self.logger.error(f"No real-time data found for {symbol}")
                return {}
            
            result = {
                "symbol": symbol,
                "timestamp": data.timestamp.isoformat(),
                "price": data.price,
                "change": data.change,
                "change_percent": data.change_percent,
                "volume": data.volume,
                "market_cap": data.market_cap,
                "bid": data.bid,
                "ask": data.ask,
                "shares_outstanding": data.shares_outstanding,
                "sentiment": data.sentiment
            }
            
            session.close()
            
            self.logger.info(f"Retrieved real-time data for {symbol}")
            return result
        except Exception as e:
            self.logger.error(f"Error getting real-time data: {e}")
            if session:
                session.close()
            return {}

if __name__ == "__main__":
    repo = Repository()
    
    historical_data = [
        {
            "date": "2023-01-01T00:00:00",
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 153.0,
            "volume": 1000000
        },
        {
            "date": "2023-01-02T00:00:00",
            "open": 153.0,
            "high": 158.0,
            "low": 152.0,
            "close": 157.0,
            "volume": 1200000
        }
    ]
    repo.store_historical_data("AAPL", historical_data)
    
    realtime_data = {
        "symbol": "AAPL",
        "timestamp": "2023-01-03T12:34:56",
        "price": 157.5,
        "change": 0.5,
        "change_percent": 0.32,
        "volume": 500000,
        "market_cap": 2500000000000,
        "bid": 157.4,
        "ask": 157.6
    }
    repo.store_realtime_data(realtime_data)
    
    retrieved_historical = repo.get_historical_data("AAPL")
    print(f"Retrieved historical data: {retrieved_historical}")
    
    retrieved_realtime = repo.get_realtime_data("AAPL")
    print(f"Retrieved real-time data: {retrieved_realtime}") 