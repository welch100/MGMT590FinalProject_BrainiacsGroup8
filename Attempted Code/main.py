import json
import logging
import requests
from google.cloud import pubsub_v1

logging.basicConfig(format='%(asctime)s.%(msecs)03dZ,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(__name__)

API_KEY = "2b7c18524e7bb12f9840cf0eb045b948"
LAT = "42.0003"
LON = "-93.5005"
TOPIC_PATH = "projects/final-project-395500/topics/finalprojectkim"

def get_weather_data():
    BASE_URL = "https://api.openweathermap.org/data/2.5/onecall?"
    url = BASE_URL + "lat=" + LAT + "&lon=" + LON + "&appid=" + API_KEY
    response = requests.get(url).json()
    return response

def parse_weather_data(weather_data):
    parsed_data = {
        "latitude": weather_data["lat"],
        "longitude": weather_data["lon"],
        "timezone": weather_data["timezone"],
        "current_temperature": weather_data["current"]["temp"],
        "current_feels_like": weather_data["current"]["feels_like"],
        "current_weather_description": weather_data["current"]["weather"][0]["description"]
        # Add more fields as needed
    }
    return parsed_data

def publish_to_pubsub(parsed_data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = TOPIC_PATH
    data = json.dumps(parsed_data).encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future.result()

def main(event, context):
    weather_data = get_weather_data()
    parsed_weather_data = parse_weather_data(weather_data)
    publish_result = publish_to_pubsub(parsed_weather_data)
    
    _logger.info("Published message ID: %s", publish_result)

if __name__ == "__main__":
    main(None, None)
