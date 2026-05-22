
from kafka import KafkaProducer
import json
import psycopg2

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Connect PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="crypto_pipeline",
    user="postgres",
    password="12345"
)

cursor = conn.cursor()

# Read latest records
cursor.execute("""
SELECT symbol, price, timestamp
FROM prices
ORDER BY timestamp DESC
LIMIT 5
""")

rows = cursor.fetchall()

# Send to Kafka
for row in rows:

    message = {
        "symbol": row[0],
        "price": float(row[1]),
        "timestamp": str(row[2])
    }

    producer.send("crypto_prices", value=message)

    print("Sent to Kafka:", message)

producer.flush()

cursor.close()
conn.close()