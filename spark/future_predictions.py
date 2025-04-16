#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from cassandra.cluster import Cluster
import socket

# 🔹 Configuration de la base Cassandra
CASSANDRA_HOST = "127.0.0.1"  # Utilisation de l'adresse IP de l'hôte local
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "hiv_data"
CASSANDRA_TABLE = "future_predictions"

def create_spark_session():
    """Créer une session Spark"""
    return (SparkSession.builder
            .appName("HIVDataProcessor")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
            .getOrCreate())

def define_schema():
    """Définir le schéma des données VIH"""
    return StructType([
        StructField("Entity", StringType(), True),
        StructField("Code", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Incidence - HIV/AIDS - Sex: Both - Age: All Ages (Number)", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

def train_model(df):
    """Entraîner un modèle de régression linéaire"""
    assembler = VectorAssembler(inputCols=["Year"], outputCol="features")
    df = assembler.transform(df)
    df = df.withColumnRenamed("Incidence - HIV/AIDS - Sex: Both - Age: All Ages (Number)", "label")
    df = df.withColumn("label", col("label").cast(DoubleType()))
    df = df.filter(col("label").isNotNull() & col("features").isNotNull())

    df.show()  # Affiche les données pour vérification
    print(f"Nombre de lignes : {df.count()}")  # Vérifie le total des lignes

    # Séparer les données en entraînement et test
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    if train_data.isEmpty():
        raise ValueError("❌ Dataset d'entraînement vide après split")

    # Entraîner le modèle
    lr = LinearRegression(featuresCol="features", labelCol="label")
    model = lr.fit(train_data)
    predictions = model.transform(test_data)

    # Évaluation du modèle
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)

    return model, rmse, r2

def write_future_predictions_to_cassandra(predictions):
    """Sauvegarder les prédictions futures dans Cassandra"""
    from cassandra.query import BatchStatement, SimpleStatement
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.set_keyspace(CASSANDRA_KEYSPACE)

        insert_stmt = session.prepare(f"""
            INSERT INTO {CASSANDRA_TABLE} (entity, code, year, predicted_cases)
            VALUES (?, ?, ?, ?)
        """)

        batch = BatchStatement()
        count = 0

        for row in predictions.collect():
            entity = row["Entity"]
            code = row["Code"]
            year = int(row["Year"])
            predicted_cases = float(row["prediction"])

            batch.add(insert_stmt, (entity, year, code, predicted_cases))
            count += 1

            if count % 20 == 0:
                session.execute(batch, timeout=10)
                batch.clear()

        # Pour le reste (moins de 20 lignes)
        if batch:
            session.execute(batch, timeout=10)

        print("✅ Données futures stockées avec succès dans Cassandra (batch de 20 lignes)")
    except Exception as e:
        print(f"❌ Erreur Cassandra : {e}")

def process_batch(spark, df):
    """Traiter un batch de données"""
    if not df.isEmpty():
        print(f"📦 Traitement du batch de données")

        entities = df.select("Entity", "Code").distinct().collect()

        for row in entities:
            entity = row["Entity"]
            code = row["Code"]
            
            filtered_df = df.filter((col("Entity") == entity) & (col("Code") == code))

            if filtered_df.count() < 2:
                continue  # Pas assez de données pour entraîner un modèle

            try:
                model, _, _ = train_model(filtered_df)

                # Transformer les données en utilisant le modèle entraîné
                assembler = VectorAssembler(inputCols=["Year"], outputCol="features")
                filtered_df = assembler.transform(filtered_df)

                # Générer les futures années
                future_years = [2026]
                future_data = spark.createDataFrame(
                    [(entity, code, y) for y in future_years],
                    ["Entity", "Code", "Year"]
                )
                future_data = assembler.transform(future_data)
                future_predictions = model.transform(future_data)

                # Remplacer les prédictions négatives par 0
                future_predictions = future_predictions.withColumn(
                    "prediction", when(col("prediction") < 0, 0).otherwise(col("prediction"))
                )

                write_future_predictions_to_cassandra(future_predictions)

            except Exception as e:
                print(f"❌ Erreur pour {entity} : {e}")

def main():
    """Point d’entrée"""
    spark = create_spark_session()  # Création de la session Spark
    schema = define_schema()

    # Lire les données depuis Kafka en mode batch
    df = (spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "hiv-data")
          .option("startingOffsets", "earliest")
          .load())

    # Convertir les données Kafka en DataFrame structuré
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col("value"), schema).alias("data")) \
                  .select("data.*")

    # Traiter les données
    parsed_df = parsed_df \
        .withColumn("Year", col("Year").cast(IntegerType())) \
        .withColumn("Incidence - HIV/AIDS - Sex: Both - Age: All Ages (Number)", col("Incidence - HIV/AIDS - Sex: Both - Age: All Ages (Number)").cast(IntegerType()))

    # Traiter le batch de données
    process_batch(spark, parsed_df)  # Passer la session Spark à la fonction

if __name__ == "__main__":
    main()

