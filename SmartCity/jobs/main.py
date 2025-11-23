import datetime
import os
import sys
import random
import uuid
import time
from idlelib.hyperparser import HyperParser
from weakref import WeakMethod

from confluent_kafka import Producer
import simplejson as json
from datetime import datetime, timedelta


LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1222}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate the movement increments
LATITUDE_INCREMENTS = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENTS = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]) / 100

# Environment Variables for configuration
KAFKA_BOOSTRAP_SERVERS = os.getenv("KAFKA_BOOSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEHATHER_TOPIC = os.getenv("WEHATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

random.seed(42)

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # Update Frequency
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'device_Id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type,
    }


def generate_traffic_camera_data(device_id, timestamp, camera_id, location):
    return {
        'id': uuid.uuid4(),
        'device_Id': device_id,
        'camera_Id': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_Id': device_id,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['sunny', 'cloudy', 'rain', 'snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQuality': random.uniform(0, 500),
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }


def simulate_vehicle_movement():
    global start_location

    # Move to Birmingham
    start_location["latitude"] += LATITUDE_INCREMENTS
    start_location["longitude"] += LONGITUDE_INCREMENTS

    # Add some randomness to simulate actual road travel
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location["latitude"], location["longitude"]),
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "make": "BMW",
        "model": "C500",
        "year": 2024,
        "fuelType": "Hybrid",
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"object of type {obj.__class__.__name__} is not JSON serializable")


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic=topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)

        gps_data = generate_gps_data(
            device_id,
            vehicle_data['timestamp']
        )

        traffic_camera_data = generate_traffic_camera_data(
            device_id,
            vehicle_data['timestamp'],
            camera_id='Nikon_Cam123',
            location=vehicle_data['location'],
        )

        weather_data = generate_weather_data(
            device_id,
            vehicle_data['timestamp'],
            vehicle_data['location'],
        )

        emergency_incident_data = generate_emergency_incident_data(
            device_id,
            vehicle_data['timestamp'],
            vehicle_data['location'],
        )

        if (
            vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
            and vehicle_data['location'][1] >= BIRMINGHAM_COORDINATES['longitude']
        ):
            print('Vehicle has reached Birmingham. Simulation ending')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEHATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOSTRAP_SERVERS,
        "error_cb": lambda err: print(f"kafka error: {err}"),
    }
    producer = Producer(producer_config)

    try:
        simulate_journey(producer, "Vehicle-CodeWithYu-123")
    except KeyboardInterrupt:
        print("Simulation ended by user")
    except Exception as e:
        print(f"unexpected error occured: {e}")
    finally:
        producer.flush()