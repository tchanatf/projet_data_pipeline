#!/bin/bash

# Topic pour l'API
docker exec -it kafka kafka-topics --create \
  --topic apiRealTimeAndFileData \
  --bootstrap-server localhost:29092 \
  --partitions 3 \
  --replication-factor 1

echo "Le topic a été crée avec succès !"