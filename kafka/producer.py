from kafka import KafkaProducer
from hdfs import InsecureClient
import json
import pandas as pd
import time

# Configuration HDFS
HDFS_URL = "http://localhost:9870"
HDFS_PATH = "/user/hiv_project/data/new-cases-of-hiv-infection.csv"

# Configuration Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "hiv-data"

# Connexion à HDFS
client = InsecureClient(HDFS_URL, user="hdfs")

# Lire le fichier depuis HDFS
with client.read(HDFS_PATH, encoding='utf-8') as file:
    df = pd.read_csv(file)

# Convertir en JSON
records = df.to_dict(orient="records")

# Initialiser le Producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoyer les messages à Kafka en simulant un flux en temps réel
for record in records:
    producer.send(TOPIC_NAME, value=record)
    print(f"Message envoyé : {record}")
    time.sleep(0.02)

producer.flush()
producer.close()
print("Tous les messages ont été envoyés.")

