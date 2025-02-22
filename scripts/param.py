
####################  Configuration des paramètres du projet ####################


# Configuration Kafka
KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'apiRealTimeAndFileData'


# Mode d'ingestion des données : 'realtime_from_api' ou 'batch'
INGESTION_MODE = "realtime_from_api" # changer en mode 'batch' si on veut ingérer un fichier plat.

# Paramètres PostgreSQL
DB_NAME = 'mydatabase'
DB_USER = 'admin'
DB_PASSWORD = 'password123'
DB_HOST = 'localhost'
DB_PORT = '5432' 