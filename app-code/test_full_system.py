import subprocess
import time
import os
import sys
import argparse
import requests
from datetime import datetime

def start_system():
    """Start the entire system."""
    print("Starting the system...")
    
    # Start Docker containers and services
    process = subprocess.Popen(
        ["python", "start_system.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for system to start
    print("Waiting for system to start...")
    time.sleep(30)
    
    return process

def stop_system(process):
    """Stop the system."""
    print("Stopping the system...")
    
    # Send Ctrl+C to the process
    process.terminate()
    
    # Wait for system to stop
    print("Waiting for system to stop...")
    process.wait()
    
    print("System stopped")

def test_data_ingestion():
    """Test data ingestion by sending sample data."""
    print("Testing data ingestion...")
    
    try:
        # Send sample data for a few companies
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
        
        for symbol in symbols:
            # Send historical data
            historical_data = {
                "symbol": symbol,
                "data": [
                    {
                        "date": (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).isoformat(),
                        "open": 100.0,
                        "high": 105.0,
                        "low": 95.0,
                        "close": 102.0,
                        "volume": 1000000
                    }
                ]
            }
            
            response = requests.post(
                "http://localhost:5001/api/v1/historical",
                json=historical_data
            )
            
            if response.status_code == 200:
                print(f"Successfully sent historical data for {symbol}")
            else:
                print(f"Error sending historical data for {symbol}: {response.text}")
            
            # Send real-time data
            realtime_data = {
                "symbol": symbol,
                "price": 102.5,
                "volume": 50000,
                "timestamp": datetime.now().isoformat(),
                "change_percent": 0.5,
                "market_cap": 1000000000
            }
            
            response = requests.post(
                "http://localhost:5001/api/v1/realtime",
                json=realtime_data
            )
            
            if response.status_code == 200:
                print(f"Successfully sent real-time data for {symbol}")
            else:
                print(f"Error sending real-time data for {symbol}: {response.text}")
            
            time.sleep(1)
        
        print("Data ingestion test completed")
        return True
    except Exception as e:
        print(f"Error testing data ingestion: {e}")
        return False

def test_data_visualization():
    """Test data visualization by accessing the dashboard."""
    print("Testing data visualization...")
    
    try:
        response = requests.get("http://localhost:5000")
        
        if response.status_code == 200:
            print("Successfully accessed the dashboard")
            return True
        else:
            print(f"Error accessing the dashboard: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error testing data visualization: {e}")
        return False

def main():
    """Main function to test the full system."""
    parser = argparse.ArgumentParser(description="Test the financial data system")
    parser.add_argument("--no-start", action="store_true", help="Don't start the system (assume it's already running)")
    
    args = parser.parse_args()
    
    process = None
    
    try:
        if not args.no_start:
            process = start_system()
        
        # Test data ingestion
        ingestion_success = test_data_ingestion()
        
        # Wait for data to be processed
        print("Waiting for data to be processed...")
        time.sleep(10)
        
        # Test data visualization
        visualization_success = test_data_visualization()
        
        # Print test results
        print("\nTest Results:")
        print(f"Data Ingestion: {'Success' if ingestion_success else 'Failure'}")
        print(f"Data Visualization: {'Success' if visualization_success else 'Failure'}")
        
        if ingestion_success and visualization_success:
            print("\nAll tests passed!")
        else:
            print("\nSome tests failed.")
        
    except KeyboardInterrupt:
        print("Test interrupted")
    finally:
        if process is not None and not args.no_start:
            stop_system(process)

if __name__ == "__main__":
    main() 