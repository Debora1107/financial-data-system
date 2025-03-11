import os
import subprocess
import time
import sys
import signal
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Define services to start
services = [
    {
        "name": "Kafka Consumer",
        "command": ["python", "real_time_processing/kafka_consumer.py"],
        "process": None
    },
    {
        "name": "Data Visualization",
        "command": ["python", "data_visualization/app.py"],
        "process": None
    }
]

# Function to handle graceful shutdown
def signal_handler(sig, frame):
    logger.info("Shutting down services...")
    for service in services:
        if service["process"] and service["process"].poll() is None:
            logger.info(f"Stopping {service['name']}...")
            service["process"].terminate()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Wait for Kafka to be ready
def wait_for_kafka():
    logger.info("Waiting for Kafka to be ready...")
    kafka_ready = False
    retries = 0
    max_retries = 30
    
    while not kafka_ready and retries < max_retries:
        try:
            # Simple check to see if Kafka is up
            result = subprocess.run(
                ["nc", "-z", "kafka", "9092"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5
            )
            if result.returncode == 0:
                kafka_ready = True
                logger.info("Kafka is ready!")
            else:
                retries += 1
                logger.info(f"Waiting for Kafka... ({retries}/{max_retries})")
                time.sleep(5)
        except Exception as e:
            retries += 1
            logger.error(f"Error checking Kafka: {e}")
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("Kafka is not available after maximum retries. Exiting.")
        sys.exit(1)

# Start all services
def start_services():
    logger.info("Starting all services...")
    
    # Wait for Kafka to be ready before starting services
    wait_for_kafka()
    
    for service in services:
        try:
            logger.info(f"Starting {service['name']}...")
            service["process"] = subprocess.Popen(
                service["command"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            logger.info(f"{service['name']} started with PID {service['process'].pid}")
        except Exception as e:
            logger.error(f"Failed to start {service['name']}: {e}")

# Monitor services and restart if needed
def monitor_services():
    while True:
        for service in services:
            if service["process"] and service["process"].poll() is not None:
                exit_code = service["process"].poll()
                logger.warning(f"{service['name']} exited with code {exit_code}. Restarting...")
                try:
                    service["process"] = subprocess.Popen(
                        service["command"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        universal_newlines=True
                    )
                    logger.info(f"{service['name']} restarted with PID {service['process'].pid}")
                except Exception as e:
                    logger.error(f"Failed to restart {service['name']}: {e}")
        
        # Check every 10 seconds
        time.sleep(10)

if __name__ == "__main__":
    logger.info("Financial Data System starting...")
    
    # Start all services
    start_services()
    
    # Monitor and restart services if needed
    monitor_services()