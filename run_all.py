import os
import sys
import subprocess
import time
import signal
import atexit

services = {
    "data_ingestion": ["python", "data_ingestion/api.py"],
    "real_time_processing": ["python", "real_time_processing/api.py"],
    "data_storage": ["python", "data_storage/api.py"],
    "data_analysis": ["python", "data_analysis/api.py"],
    "data_visualization": ["python", "data_visualization/app.py"]
}

processes = {}

def start_services():
    """Start all services."""
    print("Starting services...")
    
    for name, command in services.items():
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