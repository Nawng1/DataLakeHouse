import os
import json
import time
import requests
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables or defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC_FLOOD_WARNINGS = 'flood-warnings'
KAFKA_TOPIC_FLOOD_AREAS = 'flood-areas'
KAFKA_TOPIC_WATER_LEVELS = 'water-levels'
KAFKA_TOPIC_STATIONS = 'stations'
KAFKA_TOPIC_HISTORICAL_READINGS = 'historical-readings'

# API base URL
API_BASE_URL = "https://environment.data.gov.uk/flood-monitoring"

# Directory to save API data
API_DATA_DIR = "api-data"


def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10),
            # Adding configuration to handle large messages
            max_request_size=20971520,  # 20MB
            buffer_memory=33554432      # 32MB
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise

def save_to_file(data, filename):
    """Save JSON data to a file in the API_DATA_DIR directory"""
    if not data:
        logger.warning(f"No data to save to file {filename}")
        return None
        
    # Create output directory if it doesn't exist
    if not os.path.exists(API_DATA_DIR):
        os.makedirs(API_DATA_DIR)
        logger.info(f"Created output directory: {API_DATA_DIR}")
    
    # Add timestamp to filename to avoid overwriting (optional)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(API_DATA_DIR, f"{timestamp}_{filename}")
    
    # Save data to file
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Data saved to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to save data to {filepath}: {e}")
        return None

def fetch_api_data(endpoint, params=None):
    """Fetch data from the API and return as JSON"""
    try:
        url = f"{API_BASE_URL}/{endpoint}"
        logger.info(f"Fetching data from: {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None

def get_latest_readings():
    """Get the latest readings for all stations - useful for current status"""
    logger.info("Fetching latest readings for all stations")
    return fetch_api_data("data/readings", {"latest": True})

def get_stations_with_type(station_type=None, limit=500):
    """Get stations with optional type filter"""
    logger.info(f"Fetching stations data (type: {station_type if station_type else 'all'})")
    
    params = {"_limit": limit, "_view": "full"}  # full view includes scale information
    if station_type:
        params["type"] = station_type
    
    return fetch_api_data("id/stations", params)

def get_active_floods():
    """Get active flood warnings - useful for classification and risk analysis"""
    logger.info("Fetching active flood warnings")
    return fetch_api_data("id/floods")

def get_flood_areas_with_risk():
    """Get flood areas with risk level - useful for risk modeling"""
    logger.info("Fetching flood areas")
    return fetch_api_data("id/floodAreas")

def get_historical_readings(station_id, days=30):
    """Get historical readings for a station - useful for time series analysis"""
    logger.info(f"Fetching {days}-day historical readings for station {station_id}")
    
    # Calculate date parameters
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    params = {
        "startdate": start_date.strftime("%Y-%m-%d"),
        "enddate": end_date.strftime("%Y-%m-%d"),
        "_sorted": True
    }
    
    return fetch_api_data(f"id/stations/{station_id}/readings", params)

# Process and send data to Kafka
def send_to_kafka(producer, topic, data):
    if not data:
        logger.warning(f"No data to send to topic {topic}")
        return False
    
    try:
        future = producer.send(topic, data)
        result = future.get(timeout=60)
        logger.debug(f"Sent message to topic {topic}: {result}")
        return True
    except Exception as e:
        logger.error(f"Failed to send message to Kafka topic {topic}: {e}")
        return False

def main():
    logger.info("Starting Flood Monitoring Data ETL Process")
    
    # Initialize Kafka producer
    try:
        kafka_producer = create_kafka_producer()
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return
    
    # Run an infinite loop to continuously fetch and process data
    while True:
        try:
            # 1. Get stations with geo data (useful for visualization)
            stations = get_stations_with_type()
            if stations:
                # Save to file
                save_to_file(stations, "stations.json")
                # Send to Kafka
                send_to_kafka(kafka_producer, KAFKA_TOPIC_STATIONS, stations)
                logger.info(f"Sent stations data to Kafka topic {KAFKA_TOPIC_STATIONS}")
                
            # 2. Get river level stations separately (most useful for ML)
            river_stations = get_stations_with_type("SingleLevel")
            if river_stations:
                # Save to file
                save_to_file(river_stations, "stations_SingleLevel.json")
                # We don't need to send this to Kafka as a separate topic
                # Just use it to get historical data below
                logger.info("Retrieved river stations data for historical processing")
            
            # 3. Get latest readings for all stations
            latest_readings = get_latest_readings()
            if latest_readings:
                # Save to file
                save_to_file(latest_readings, "latest_readings.json")
                # Send to Kafka
                send_to_kafka(kafka_producer, KAFKA_TOPIC_WATER_LEVELS, latest_readings)
                logger.info(f"Sent latest readings data to Kafka topic {KAFKA_TOPIC_WATER_LEVELS}")
            
            # 4. Get active flood warnings (for classification/ML)
            flood_warnings = get_active_floods()
            if flood_warnings:
                # Save to file
                save_to_file(flood_warnings, "active_floods.json")
                # Send to Kafka
                send_to_kafka(kafka_producer, KAFKA_TOPIC_FLOOD_WARNINGS, flood_warnings)
                logger.info(f"Sent flood warnings data to Kafka topic {KAFKA_TOPIC_FLOOD_WARNINGS}")
            
            # 5. Get flood areas (for risk analysis)
            flood_areas = get_flood_areas_with_risk()
            if flood_areas:
                # Save to file
                save_to_file(flood_areas, "flood_areas.json")
                # Send to Kafka
                send_to_kafka(kafka_producer, KAFKA_TOPIC_FLOOD_AREAS, flood_areas)
                logger.info(f"Sent flood areas data to Kafka topic {KAFKA_TOPIC_FLOOD_AREAS}")
            
            # 6. Get historical readings for selected stations
            if river_stations and "items" in river_stations:
                # Select up to 5 river monitoring stations for historical data
                samples = min(5, len(river_stations["items"]))
                logger.info(f"Selecting {samples} sample stations for historical data...")
                
                for i, station in enumerate(river_stations["items"][:samples]):
                    station_id = station.get('@id', '').split('/')[-1]
                    if station_id:
                        historical_data = get_historical_readings(station_id, days=30)
                        if historical_data:
                            # Save to file
                            save_to_file(historical_data, f"historical_{station_id}_30d.json")
                            # Send to Kafka
                            send_to_kafka(kafka_producer, KAFKA_TOPIC_HISTORICAL_READINGS, historical_data)
                            logger.info(f"Sent historical data for station {station_id} to Kafka topic {KAFKA_TOPIC_HISTORICAL_READINGS}")
            
            logger.info("Completed data fetch cycle, sleeping for 15 minutes")
            time.sleep(900)  # Sleep for 15 minutes between cycles
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(60)  # Sleep for a minute before retrying on error

if __name__ == "__main__":
    main() 