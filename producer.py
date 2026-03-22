from kafka import KafkaProducer
import pandas as pd
import json
import time

# -------------------------------
# Kafka Producer
# -------------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# -------------------------------
# Read Dataset
# -------------------------------
df = pd.read_csv("processed.csv")

print("Producer started... Press Ctrl+C to stop")

# -------------------------------
# Send Data
# -------------------------------
try:
    for _, row in df.iterrows():
        data = {
            'Amount': row['Amount'],
            'Time': row['Time'],
            'hour': row['hour'],
            'txn_count_1hr': row['txn_count_1hr'],
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'rolling_avg': row['rolling_avg'],
            'amount_deviation': row['amount_deviation'],
            'distance_km': row['distance_km'],
            'velocity': row['velocity'],
            'high_amount': row['high_amount']
        }

        producer.send("fraud", value=data)
        print("📨 Sent:", data)

        time.sleep(2)

except KeyboardInterrupt:
    print("\n Producer stopped by user")

finally:
    producer.flush()
    producer.close()
    print("Producer closed cleanly")