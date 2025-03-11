import sys
import os
import json
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger

from models import Stock, HistoricalData, RealtimeData, init_db
from repository import Repository

logger = get_logger("data_storage_test")

def test_models():
    """Test the database models."""
    logger.info("Testing database models...")
    
    session = init_db()
    
    if not session:
        logger.error("Failed to initialize database")
        return False
    
    try:
        logger.info("Testing Stock model...")
        stock = Stock(symbol="TEST", name="Test Stock", sector="Technology", industry="Software")
        session.add(stock)
        session.commit()
        
        created_stock = session.query(Stock).filter_by(symbol="TEST").first()
        if created_stock:
            logger.info(f"Successfully created stock: {created_stock}")
        else:
            logger.error("Failed to create stock")
            return False
        
        logger.info("Testing HistoricalData model...")
        historical_data = HistoricalData(
            stock_id=created_stock.id,
            date=datetime.now(),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            ma5=102.0,
            ma20=101.0,
            daily_return=0.03,
            volatility=0.02,
            rsi=60.0
        )
        session.add(historical_data)
        session.commit()
        
        created_historical = session.query(HistoricalData).filter_by(stock_id=created_stock.id).first()
        if created_historical:
            logger.info(f"Successfully created historical data: {created_historical}")
        else:
            logger.error("Failed to create historical data")
            return False
        
        logger.info("Testing RealtimeData model...")
        realtime_data = RealtimeData(
            stock_id=created_stock.id,
            timestamp=datetime.now(),
            price=103.5,
            change=0.5,
            change_percent=0.49,
            volume=500000,
            market_cap=1000000000000,
            bid=103.4,
            ask=103.6,
            shares_outstanding=9661835000,
            sentiment="neutral"
        )
        session.add(realtime_data)
        session.commit()
        
        created_realtime = session.query(RealtimeData).filter_by(stock_id=created_stock.id).first()
        if created_realtime:
            logger.info(f"Successfully created real-time data: {created_realtime}")
        else:
            logger.error("Failed to create real-time data")
            return False
        
        session.delete(created_realtime)
        session.delete(created_historical)
        session.delete(created_stock)
        session.commit()
        
        logger.info("Database models tests completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error testing database models: {e}")
        return False
    finally:
        session.close()

def test_repository():
    """Test the repository."""
    logger.info("Testing repository...")
    
    repo = Repository()
    
    logger.info("Testing store_historical_data and get_historical_data...")
    historical_data = [
        {
            "date": datetime.now().isoformat(),
            "open": 200.0,
            "high": 205.0,
            "low": 199.0,
            "close": 203.0,
            "volume": 2000000,
            "ma5": 202.0,
            "ma20": 201.0,
            "daily_return": 0.015,
            "volatility": 0.01,
            "rsi": 55.0
        }
    ]
    
    success = repo.store_historical_data("REPO_TEST", historical_data)
    if success:
        logger.info("Successfully stored historical data")
    else:
        logger.error("Failed to store historical data")
        return False
    
    retrieved_historical = repo.get_historical_data("REPO_TEST")
    if retrieved_historical:
        logger.info(f"Successfully retrieved historical data: {retrieved_historical}")
    else:
        logger.error("Failed to retrieve historical data")
        return False
    
    logger.info("Testing store_realtime_data and get_realtime_data...")
    realtime_data = {
        "symbol": "REPO_TEST",
        "timestamp": datetime.now().isoformat(),
        "price": 203.5,
        "change": 0.5,
        "change_percent": 0.25,
        "volume": 1000000,
        "market_cap": 2000000000000,
        "bid": 203.4,
        "ask": 203.6,
        "shares_outstanding": 9823500000,
        "sentiment": "positive"
    }
    
    success = repo.store_realtime_data(realtime_data)
    if success:
        logger.info("Successfully stored real-time data")
    else:
        logger.error("Failed to store real-time data")
        return False
    
    retrieved_realtime = repo.get_realtime_data("REPO_TEST")
    if retrieved_realtime:
        logger.info(f"Successfully retrieved real-time data: {retrieved_realtime}")
    else:
        logger.error("Failed to retrieve real-time data")
        return False
    
    logger.info("Repository tests completed successfully")
    return True

def run_tests():
    """Run all tests."""
    logger.info("Starting Data Storage Service tests...")
    
    if not test_models():
        logger.error("Database models tests failed")
        return False
    
    if not test_repository():
        logger.error("Repository tests failed")
        return False
    
    logger.info("All tests completed successfully")
    return True

if __name__ == "__main__":
    run_tests() 