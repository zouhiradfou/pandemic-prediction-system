# Pandemic Prediction System

A Big Data architecture for predicting pandemic spread patterns using historical HIV/AIDS data.

## Architecture Overview

This system integrates five major Big Data technologies to create an end-to-end pipeline for processing epidemiological data:

- **Data Collection**: Raw pandemic data stored in HDFS
- **Kafka**: Real-time data streaming pipeline
- **Spark Streaming**: Processing and machine learning predictions
- **Cassandra**: Persistent storage of predictions and results
- **Streamlit**: Interactive data visualization dashboard

## Technologies Used

- **Apache Hadoop (HDFS)**
- **Apache Kafka**
- **Apache Spark**
- **Cassandra**
- **Streamlit**

## Installation & Setup


## Usage

### Step 1: Load Data into HDFS
  1. Démarrer HDFS
start-dfs.sh
  2. Créer un répertoire HDFS pour les données
hdfs dfs -mkdir -p /user/hiv_project/data
  3. Charger le fichier CSV dans HDFS
hdfs dfs -put new-cases-of-hiv-infection.csv /user/hiv_project/data/

### Step 2: Start the Data Pipeline
Start each component in the following order:

Start Zookeeper and Kafka
Create Kafka topic
Start the Kafka producer
Start the Spark consumer
Start the Streamlit visualization

### Step 3: Access the Dashboard
Open your web browser and navigate to:

bash:
http://localhost:8501

#### The dashboard allows you to:

Filter data by country and time period

View trend analyses of pandemic spread

Compare real data with predictions

Explore geographical distribution of cases
