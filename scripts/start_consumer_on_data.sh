#!/bin/bash

# Variables
SPARK_CONTAINER="spark-master"
SPARK_MASTER="spark://spark-master:7077"
SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
SPARK_EXECUTOR_MEMORY="1g"
SPARK_DRIVER_MEMORY="512m"
CONSUMER_SCRIPT="/app/scripts/consumer_process_data.py"

# Commande pour lancer le consumer Spark
docker exec -it $SPARK_CONTAINER /opt/bitnami/spark/bin/spark-submit \
  --master $SPARK_MASTER \
  --packages $SPARK_PACKAGES \
  --conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY \
  --conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
  --conf spark.logLevel=DEBUG \
  $CONSUMER_SCRIPT

# Vérification que le consumer a été lancé
if [ $? -eq 0 ]; then
  echo "Consumer Spark lancé avec succès."
else
  echo "Erreur lors du lancement du consumer Spark."
fi