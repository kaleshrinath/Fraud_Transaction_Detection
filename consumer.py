import os

from kafka import KafkaConsumer
import json
import pickle
import numpy as np
from pymongo import MongoClient
from urllib.parse import quote_plus
from dotenv import load_dotenv

# -------------------------------
# Load Environment Variables
# -------------------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# -------------------------------
# MongoDB Connection
# -------------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print("Connected to MongoDB")

# -------------------------------
# Load ML Model
# -------------------------------
with open("fraud_model.pkl", "rb") as f:
    model = pickle.load(f)

print("Model loaded")

# -------------------------------
# Kafka Consumer
# -------------------------------
def deserialize_value(x):
    try:
        return json.loads(x.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        print(f" Skipping invalid message: {x}")
        return None

consumer = KafkaConsumer(
    'fraud',
    bootstrap_servers='localhost:9092',
    value_deserializer=deserialize_value,
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Listening to Kafka topic...")

# -------------------------------
# Process Messages
# -------------------------------
try:
    for message in consumer:
        raw_data = message.value
        if raw_data is None:
            continue

        try:
            # Extract only the 11 features
            data = {k: float(raw_data[k]) for k in [
                'Amount', 'Time', 'hour', 'txn_count_1hr', 'latitude',
                'longitude', 'rolling_avg', 'amount_deviation',
                'distance_km', 'velocity', 'high_amount'
            ]}

            # -------------------------------
            # Prepare Features for ML Model
            # -------------------------------
            features = np.array([[
                data['Amount'],
                data['Time'],
                data['hour'],
                data['txn_count_1hr'],
                data['latitude'],
                data['longitude'],
                data['rolling_avg'],
                data['amount_deviation'],
                data['distance_km'],
                data['velocity'],
                data['high_amount']
            ]])

            # -------------------------------
            # Prediction
            # -------------------------------
            prob = float(model.predict_proba(features)[0][1])
            prediction = "FRAUD" if prob > 0.15 else "NOT_FRAUD"

            # -------------------------------
            # Add prediction info to data
            # -------------------------------
            data["prediction"] = prediction
            data["fraud_probability"] = prob

            # -------------------------------
            # Insert into MongoDB
            # -------------------------------
            collection.insert_one(data)

            print("Saved to MongoDB:", data)

        except KeyError as ke:
            print(f"Missing feature in message: {ke}")
        except Exception as e:
            print("Error processing message:", e)

except KeyboardInterrupt:
    print("Consumer stopped by user")

finally:
    consumer.close()