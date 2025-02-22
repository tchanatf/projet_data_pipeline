import json
import requests
from confluent_kafka import Producer
import pandas as pd
import time
import io

# Configuration Kafka
API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/investissements-d'avenir-partenaires/full"
KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'apiRealTimeAndFileData'

# Mode d'ingestion des données : 'realtime_from_api' ou 'batch'
INGESTION_MODE = "realtime_from_api" # changer en mode 'batch' si on veut ingérer un fichier plat.

# fichier en mode bat
# ch (fichier plat s'il existe)

BATCH_FILE_PATH = 'dossier/fichiers/data.csv'

# Configuration du producer Kafka
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'enable.idempotence': True,  # Évite la duplication de messages
    'message.max.bytes': 10495760
}
producer = Producer(producer_config)
    
# Récupère les données de l'API en temps réel
def get_data_from_api():

    try:
        response = requests.get(API_URL)
        response.raise_for_status()

        csv_data = response.text
        df = pd.read_csv(io.StringIO(csv_data), delimiter= ",", quotechar='"', on_bad_lines="skip")

        df = df.rename(columns={
            "Code projet": "Code_projet",
            "Libellé projet": "Libellé_projet",
            "Type de projet": "Type_de_projet",
            "Appel à projet": "Appel_à_projet",
            "Date de dépôt d'un dossier complet": "Date_de_dépôt_d_un_dossier_complet",
            "Date de décision initiale PM": "Date_de_décision_initiale_PM",
            "Date de notification": "Date_de_notification",
            "Description courte": "Description_courte",
            "Durée du projet (mois)": "Durée_du_projet_mois",
            "Montants autorisés Subventions": "Montants_autorisés_Subventions",
            "Montants autorisés AR": "Montants_autorisés_AR",
            "Total autorisé": "Total_autorisé",
            "Total autorisé avant indus": "Total_autorisé_avant_indus",
            "Total contractualisé": "Total_contractualisé",
            "Total contractualisé avant indus": "Total_contractualisé_avant_indus",
            "Total indus": "Total_indus",
            "Type Etablissement": "Type_Etablissement",
            "Code INSEE Etablissement": "Code_INSEE_Etablissement",
            "Libelle Commune Etablissement": "Libelle_Commune_Etablissement",
            "Code NAF": "Code_NAF",
            "Longitude Commune Etablissement": "Longitude_Commune_Etablissement",
            "Latitude Commune Etablissement": "Latitude_Commune_Etablissement",
            "Code Département" : "Code_Département",
            "Libellé Département": "Libellé_Département",
            "Code Région": "Code_Région",
            "Libellé Région": "Libellé_Région",
            "Code EPCI": "Code_EPCI"
        })

        data = df.to_dict(orient='records')

        return data
    
    except requests.exceptions.RequestException as e:
        print(f"Les données n'ont pas été récupérées de l'API: {e}")
        return []
    
# Possibilité de récuperer en mode batch
def get_data_from_batch():
    try:
        df = pd.read_csv(BATCH_FILE_PATH, delimiter=";")
        return df.to_dict(orient='records')
    
    except FileExistsError:
        print(f"fichier non trouvé: {BATCH_FILE_PATH}")
        return []

# Envoyer les données consommées à Kafka en fonction de sa source 
# (temps réel -> API ou Batch -> fichiers plats)

def send_to_kafka(data, type_of_source):

    headers = [("source_type", type_of_source)]
    
    if not data:
        print("Pas de données à envoyer")
        return

    # data_to_json = json.dumps(data, ensure_ascii=False)

    for record in data:
        producer.produce(
            KAFKA_TOPIC, 
            json.dumps(record, ensure_ascii=False).encode('utf-8'),
            headers = headers
        )

    producer.flush()
    print(f"Envoi de {len(data)} lignes de données vers Kafka en mode ({type_of_source})")

def main():
    mode_ingestion_data = INGESTION_MODE

    if mode_ingestion_data == "realtime_from_api":
        print("mode de recuperation en temps reel à partir de l'API. Envoie toutes 10 secondes")

        try:
            while True:
                data = get_data_from_api()
                send_to_kafka(data, type_of_source="realtime_from_api")
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nInterruption du producer Kafka par l'utilisateur. Arrêt en cours...")

    elif mode_ingestion_data == "batch":
        print("mode de recuperation en batch. Envoie une seule fois")
        data = get_data_from_batch()
        send_to_kafka(data, type_of_source="batch")

    else:
        print("Mode d'ingestion pas disponible : Utilisez le mode realime ou batch")


if __name__ == "__main__":
    main()
