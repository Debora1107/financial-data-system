import os
import sys
import subprocess
import time
import signal
import atexit
import platform

# Determine the Python executable to use (with venv)
if platform.system() == "Windows":
    PYTHON_EXECUTABLE = os.path.join(os.getcwd(), "venv", "Scripts", "python.exe")
else:
    PYTHON_EXECUTABLE = os.path.join(os.getcwd(), "venv", "bin", "python")

# Check if the Python executable exists
if not os.path.exists(PYTHON_EXECUTABLE):
    print(f"Warning: Virtual environment Python executable not found at {PYTHON_EXECUTABLE}")
    print("Falling back to system Python")
    PYTHON_EXECUTABLE = "python"

services = {
    "data_ingestion": [PYTHON_EXECUTABLE, "data_ingestion/api.py"],
    "real_time_processing": [PYTHON_EXECUTABLE, "real_time_processing/api.py"],
    "kafka_consumer": [PYTHON_EXECUTABLE, "real_time_processing/kafka_consumer.py"],
    "data_storage": [PYTHON_EXECUTABLE, "data_storage/api.py"],
    "data_analysis": [PYTHON_EXECUTABLE, "data_analysis/api.py"],
    "data_visualization": [PYTHON_EXECUTABLE, "data_visualization/app.py"]
}

processes = {}

def start_services():
    """Start all services."""
    print("Starting services...")
    
    delayed_services = ["kafka_consumer"]
    regular_services = {k: v for k, v in services.items() if k not in delayed_services}
    
    for name, command in regular_services.items():
        print(f"Starting {name}...")
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        processes[name] = process
        
        print(f"{name} started with PID {process.pid}")
        
        time.sleep(1)
    
    print("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    for name in delayed_services:
        command = services[name]
        print(f"Starting {name}...")
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        processes[name] = process
        
        print(f"{name} started with PID {process.pid}")
        
        time.sleep(1)
    
    print("All services started")

def stop_services():
    """Stop all services."""
    print("Stopping services...")
    
    for name, process in processes.items():
        print(f"Stopping {name} (PID {process.pid})...")
        
        os.kill(process.pid, signal.SIGTERM)
        
        process.wait()
        
        print(f"{name} stopped")
    
    print("All services stopped")

def monitor_services():
    """Monitor services and restart if needed."""
    while True:
        try:
            for name, process in list(processes.items()):
                if process.poll() is not None:
                    print(f"{name} has terminated unexpectedly")
                    
                    stdout, stderr = process.communicate()
                    print(f"{name} stdout: {stdout}")
                    print(f"{name} stderr: {stderr}")
                    
                    print(f"Restarting {name}...")
                    
                    process = subprocess.Popen(
                        services[name],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    
                    processes[name] = process
                    
                    print(f"{name} restarted with PID {process.pid}")
            
            time.sleep(5)
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    atexit.register(stop_services)
    
    start_services()
    
    try:
        monitor_services()
    except KeyboardInterrupt:
        pass
    
    stop_services() 