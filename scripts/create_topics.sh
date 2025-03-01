#!/bin/bash

# Variables
KAFKA_CONTAINER="kafka"
TOPIC_NAME="apiRealTimeAndFileData"
BOOTSTRAP_SERVER="kafka:9092"
PARTITIONS=3
REPLICATION_FACTOR=1

# Commande pour créer le topic Kafka
docker exec $KAFKA_CONTAINER kafka-topics --create \
  --if-not-exists \
  --topic $TOPIC_NAME \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

# Vérification que le topic a été créé
if [ $? -eq 0 ]; then
  echo "Topic $TOPIC_NAME créé avec succès."
else
  echo "Erreur lors de la création du topic $TOPIC_NAME."
fi