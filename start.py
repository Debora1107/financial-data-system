import sys
import os
import subprocess
import time
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_logger

logger = get_logger("start")

def start_system(use_docker=False, init_db=False, load_data=False):
    """Start the financial data system."""
    logger.info("Starting the financial data system...")
    
    if use_docker:
        logger.info("Starting with Docker Compose...")
        
        subprocess.run(["docker-compose", "up", "-d", "--build"])
        
        logger.info("Waiting for services to start...")
        time.sleep(10)
        
        if init_db:
            logger.info("Initializing database...")
            subprocess.run(["docker-compose", "exec", "data_storage", "python", "init_db.py"])
        
        if load_data:
            logger.info("Loading sample data...")
            subprocess.run(["docker-compose", "exec", "data_ingestion", "python", "load_sample_data.py"])
        
        logger.info("System started successfully")
        logger.info("Access the dashboard at http://localhost:5000")
    else:
        logger.info("Starting locally...")
        
        if init_db:
            logger.info("Initializing database...")
            subprocess.run(["python", "init_db.py"])
        
        logger.info("Starting all services...")
        subprocess.Popen(["python", "run_all.py"])
        
        logger.info("Waiting for services to start...")
        time.sleep(10)
        
        if load_data:
            logger.info("Loading sample data...")
            subprocess.run(["python", "load_sample_data.py"])
        
        logger.info("System started successfully")
        logger.info("Access the dashboard at http://localhost:5000")
        logger.info("Press Ctrl+C to stop the system")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping the system...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the financial data system")
    parser.add_argument("--docker", action="store_true", help="Use Docker Compose")
    parser.add_argument("--init-db", action="store_true", help="Initialize the database")
    parser.add_argument("--load-data", action="store_true", help="Load sample data")
    
    args = parser.parse_args()
    
    start_system(use_docker=args.docker, init_db=args.init_db, load_data=args.load_data) 