import json
import redis.client
import requests
from confluent_kafka import Producer
import pandas as pd
import redis
import time
import io
import unidecode
import re

# Connexion à Redis

redis_client = redis.Redis(host= "redis", 
                           port=6379,
                           db=0, 
                           decode_responses=True)

# Configuration Kafka
API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/investissements-d'avenir-partenaires/full"
BATCH_FILE_PATH = "lien_du_fichier_plat"

KAFKA_TOPIC = 'apiRealTimeAndFileData'

# Mode d'ingestion des données : 'realtime_from_api' ou 'batch'
INGESTION_MODE = "realtime_from_api" # changer en mode 'batch' si on veut ingérer un fichier plat.


# Configuration du producer Kafka
producer_config = {
    'bootstrap.servers': 'kafka:9092',
    'enable.idempotence': True,  # Évite la duplication de messages
    'message.max.bytes': 10495760
}
producer = Producer(producer_config)

#---------------- FONCTION DE NORMALISATION DES CHAMPS ---------------------------------------

def normalize_column_name(col_name):
    col_name = unidecode.unidecode(col_name)  # Supprimer accents
    col_name = re.sub(r"[^\w\s]", "", col_name)  # Supprimer caractères spéciaux (parenthèses, etc.)
    col_name = re.sub(r"\s+", "_", col_name)  # Remplacer espaces par underscores
    col_name = col_name.lower()

    return col_name


#---------------- RECUPERATION DES DONNEES API ---------------------------------------

# Récupère les données de l'API et renvoie uniquement les nouvelles lignes non encore envoyées 
def get_data_from_api():

    try:
        response = requests.get(API_URL)
        response.raise_for_status()

        csv_data = response.text
        df = pd.read_csv(io.StringIO(csv_data), delimiter= ",", quotechar='"', on_bad_lines="skip")

        # Normalisation automatique des noms de colonnes
        df.columns = [normalize_column_name(col) for col in df.columns]

        # print("Colonnes normalisées :", df.columns.tolist())

        data = df.to_dict(orient='records')

        # Récuperer les anciennes données depuis Redis
        last_data_json = redis_client.get("last_api_data")
        last_data = json.loads(last_data_json) if last_data_json else []

        # Cherche de nouvelles lignes 
        last_data_set = {json.dumps(item, sort_keys=True) for item in last_data}
        new_data = [item for item in data if json.dumps(item, sort_keys=True) not in last_data_set]

        if new_data:
            # Sauvegarder le nouvel état dans Redis
            redis_client.set("last_api_data", json.dumps(data, ensure_ascii=False))

        # Retoune uniquement les nouvelles lignes
        return new_data
    
    except requests.exceptions.RequestException as e:
        print(f"Les données n'ont pas été récupérées de l'API: {e}")
        return []
    
# ------------------- RECUPERATION DES DONNEES BATCH (POSSIBILITE) -------------------
def get_data_from_batch():
    try:
        df = pd.read_csv(BATCH_FILE_PATH, delimiter=";")
        return df.to_dict(orient='records')
    
    except FileExistsError:
        print(f"fichier non trouvé: {BATCH_FILE_PATH}")
        return []
    
# ------------------------ ENVOI A KAFKA -----------------------------
def send_to_kafka(data, type_of_source):

    headers = [("source_type", type_of_source)]
    
    # Envoie uniquement les nouvelles lignes à Kafka
    if not data:
        print("Pas de nouvelles données à envoyer")
        return
    
    count_sent = 0

    for record in data:

        # Utilisation d'un hash JSON comme clé unique
        key = json.dumps(record, sort_keys=True)

        producer.produce(
            KAFKA_TOPIC, 
            value = json.dumps(record, ensure_ascii=False).encode('utf-8'),
            headers = headers
        )
        count_sent +=1

    producer.flush()
    print(f"{count_sent} nouvelles lignes envoyées à Kafka Envoi")

# ------------------- MAIN -------------------
def main():
    mode_ingestion_data = INGESTION_MODE

    if mode_ingestion_data == "realtime_from_api":
        print("Mode streaming :  envoie des nouvelles données toutes les 10 secondes")

        try:
            while True:
                new_data = get_data_from_api()
                send_to_kafka(new_data, type_of_source="realtime_from_api")
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nInterruption du producer Kafka par l'utilisateur. Arrêt en cours...")
        except Exception as e:
            print(f"Erreur lors de l'envoi des données: {e}")

    elif mode_ingestion_data == "batch":
        print("mode de recuperation en batch. Envoie une seule fois")
        data = get_data_from_batch()
        send_to_kafka(data, type_of_source="batch")

    else:
        print("Mode d'ingestion pas disponible : Utilisez le mode realime ou batch")


if __name__ == "__main__":
    main()