import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_logger

from data_storage.models import Base, Stock, init_db

logger = get_logger("init_db")

def initialize_database():
    """Initialize the database."""
    logger.info("Initializing database...")
    
    session = init_db()
    
    if not session:
        logger.error("Failed to initialize database")
        return False
    
    try:
        stocks = [
            {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Technology", "industry": "Consumer Electronics"},
            {"symbol": "MSFT", "name": "Microsoft Corporation", "sector": "Technology", "industry": "Software"},
            {"symbol": "GOOG", "name": "Alphabet Inc.", "sector": "Technology", "industry": "Internet Content & Information"},
            {"symbol": "AMZN", "name": "Amazon.com, Inc.", "sector": "Consumer Cyclical", "industry": "Internet Retail"},
            {"symbol": "META", "name": "Meta Platforms, Inc.", "sector": "Technology", "industry": "Internet Content & Information"}
        ]
        
        for stock_data in stocks:
            existing = session.query(Stock).filter_by(symbol=stock_data["symbol"]).first()
            
            if not existing:
                stock = Stock(**stock_data)
                session.add(stock)
                logger.info(f"Created stock: {stock_data['symbol']}")
        
        session.commit()
        
        session.close()
        
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        if session:
            session.rollback()
            session.close()
        return False

if __name__ == "__main__":
    initialize_database() 