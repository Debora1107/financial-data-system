import sys
import os
from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, get_db_url


logger = get_logger("data_storage_models")


Base = declarative_base()

class Stock(Base):
    """Stock model."""
    
    __tablename__ = "stocks"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=True)
    sector = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    historical_data = relationship("HistoricalData", back_populates="stock")
    realtime_data = relationship("RealtimeData", back_populates="stock")
    
    def __repr__(self):
        return f"<Stock(symbol='{self.symbol}', name='{self.name}')>"

class HistoricalData(Base):
    """Historical data model."""
    
    __tablename__ = "historical_data"
    
    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    
    ma5 = Column(Float, nullable=True)
    ma20 = Column(Float, nullable=True)
    daily_return = Column(Float, nullable=True)
    volatility = Column(Float, nullable=True)
    rsi = Column(Float, nullable=True)
    
    created_at = Column(DateTime, default=datetime.now)
    
    stock = relationship("Stock", back_populates="historical_data")
    
    def __repr__(self):
        return f"<HistoricalData(stock='{self.stock.symbol}', date='{self.date}', close='{self.close}')>"

class RealtimeData(Base):
    """Real-time data model."""
    
    __tablename__ = "realtime_data"
    
    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    price = Column(Float, nullable=False)
    change = Column(Float, nullable=True)
    change_percent = Column(Float, nullable=True)
    volume = Column(Integer, nullable=True)
    market_cap = Column(Float, nullable=True)
    bid = Column(Float, nullable=True)
    ask = Column(Float, nullable=True)
    
    shares_outstanding = Column(Float, nullable=True)
    sentiment = Column(String, nullable=True)
    
    created_at = Column(DateTime, default=datetime.now)
    
    stock = relationship("Stock", back_populates="realtime_data")
    
    def __repr__(self):
        return f"<RealtimeData(stock='{self.stock.symbol}', timestamp='{self.timestamp}', price='{self.price}')>"

def init_db():
    """Initialize the database."""
    try:
        engine = create_engine(get_db_url())
        
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        logger.info("Database initialized successfully")
        return session
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return None

if __name__ == "__main__":
    session = init_db()
    
    if session:
        stock = Stock(symbol="AAPL", name="Apple Inc.", sector="Technology", industry="Consumer Electronics")
        session.add(stock)
        session.commit()
        
        historical_data = HistoricalData(
            stock_id=stock.id,
            date=datetime.now(),
            open=150.0,
            high=155.0,
            low=149.0,
            close=153.0,
            volume=1000000,
            ma5=152.0,
            ma20=150.0,
            daily_return=0.02,
            volatility=0.015,
            rsi=65.0
        )
        session.add(historical_data)
        
        realtime_data = RealtimeData(
            stock_id=stock.id,
            timestamp=datetime.now(),
            price=153.0,
            change=3.0,
            change_percent=2.0,
            volume=1000000,
            market_cap=2500000000000,
            bid=152.9,
            ask=153.1,
            shares_outstanding=16339730000,
            sentiment="positive"
        )
        session.add(realtime_data)
        
        session.commit()
        
        session.close()
        
        logger.info("Example data created successfully") 