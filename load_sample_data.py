import yfinance as yf
import pandas as pd

def get_all_symbols():
    """Get list of all available stock symbols."""
    tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp500 = tables[0]
    symbols = sp500['Symbol'].tolist()
    return symbols

def load_sample_data():
    """Load data for all available stocks."""
    logger.info("Loading data for all stocks...")
    
    symbols = get_all_symbols()
    logger.info(f"Found {len(symbols)} symbols")
    
    try:
        for symbol in symbols:
            logger.info(f"Processing data for {symbol}...")
            
            data = generate_sample_data(symbol, 30)
            logger.info(f"Generated {len(data)} historical records for {symbol}")
            
            try:
                response = requests.post(
                    f"{STORAGE_URL}/api/v1/historical",
                    json={"symbol": symbol, "data": data},
                    timeout=5
                )
                if response.status_code == 200:
                    logger.info(f"Successfully stored historical data for {symbol}")
                else:
                    logger.error(f"Error storing historical data for {symbol}: {response.text}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error storing data for {symbol}: {e}")
                continue
            
            realtime_data = generate_sample_realtime_data(symbol)
            logger.info(f"Generated real-time data for {symbol}")
            
            try:
                response = requests.post(
                    f"{STORAGE_URL}/api/v1/realtime",
                    json={"symbol": symbol, "data": realtime_data},
                    timeout=5
                )
                if response.status_code == 200:
                    logger.info(f"Successfully stored real-time data for {symbol}")
                else:
                    logger.error(f"Error storing real-time data for {symbol}: {response.text}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error storing realtime data for {symbol}: {e}")
                continue
        
        logger.info("All stock data loaded successfully")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error loading data: {e}")
        return False

def generate_sample_data(symbol, days):
    """Generate sample historical data."""
    data = []
    base_date = datetime.now() - timedelta(days=days)
    
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        base_price = info.get('previousClose', 100.0)
    except:
        base_price = 100.0  
    
   
    ...

def generate_sample_realtime_data(symbol):
    """Generate sample real-time data."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        price = info.get('regularMarketPrice', 100.0)
        market_cap = info.get('marketCap', 1000000000)
    except:
        price = 100.0
        market_cap = 1000000000
    
   
    ...