import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_logger(name):
    """Get a logger with the given name."""
    return logging.getLogger(name)

def load_config():
    """Load configuration from environment variables."""
    return {
        "yahoo_finance_api_key": os.getenv("YAHOO_FINANCE_API_KEY"),
        "db": {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "name": os.getenv("DB_NAME", "financial_data"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgres"),
        },
        "rabbitmq": {
            "host": os.getenv("RABBITMQ_HOST", "localhost"),
            "port": int(os.getenv("RABBITMQ_PORT", 5672)),
            "user": os.getenv("RABBITMQ_USER", "guest"),
            "password": os.getenv("RABBITMQ_PASSWORD", "guest"),
        },
        "services": {
            "data_ingestion": {
                "port": int(os.getenv("DATA_INGESTION_PORT", 5001)),
            },
            "real_time_processing": {
                "port": int(os.getenv("REAL_TIME_PROCESSING_PORT", 5002)),
            },
            "data_storage": {
                "port": int(os.getenv("DATA_STORAGE_PORT", 5003)),
            },
            "data_analysis": {
                "port": int(os.getenv("DATA_ANALYSIS_PORT", 5004)),
            },
            "data_visualization": {
                "port": int(os.getenv("DATA_VISUALIZATION_PORT", 5000)),
            },
        },
    }

def get_db_url():
    """Get the database URL for SQLAlchemy."""
    config = load_config()
    db = config["db"]
    return f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"

def serialize_datetime(obj):
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def json_dumps(obj):
    """Serialize object to JSON string with datetime support."""
    return json.dumps(obj, default=serialize_datetime)

def json_loads(s):
    """Deserialize JSON string to object."""
    return json.loads(s) 