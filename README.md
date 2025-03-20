# Pipeline de Données en Temps Réel avec Kafka, Spark, Airflow et Power BI

![Badge](https://img.shields.io/badge/Technologies-Kafka%20%7C%20Spark%20%7C%20Airflow%20%7C%20Power%20BI-blue)
![Badge](https://img.shields.io/badge/Status-Complété-brightgreen)

Ce projet est un pipeline de données complet conçu pour ingérer, traiter et visualiser des données en temps réel. Il utilise des technologies modernes comme **Apache Kafka**, **Docker**,  **Apache Spark**, **Apache Airflow**, **Cassandra**, **Redis**, **PySpark**   
et **Power BI** pour créer un système robuste et scalable.

---

## 📋 Table des Matières

1. [Aperçu du Projet](#-aperçu-du-projet)
2. [Technologies Utilisées](#-technologies-utilisées)
3. [Structure du Projet](#-structure-du-projet)
4. [Installation et Déploiement](#-installation-et-déploiement)
5. [Flux de Données](#-flux-de-données)
6. [Visualisation avec Power BI](#-visualisation-avec-power-bi)

---

## 🚀 Aperçu du Projet

Ce projet vise à démontrer la capacité à gérer des flux de données complexes en temps réel. Il comprend :

- **Ingestion de données** : Récupération de données à partir de l'API publique de l'ADEME et envoi vers Kafka.
- **Traitement de données** : Transformation et traitement des données avec Apache Spark.
- **Stockage** : Stockage des données traitées dans Cassandra.
- **Orchestration** : Automatisation des tâches avec Apache Airflow.
- **Visualisation** : Création de tableaux de bord interactifs avec Power BI.

---

## 🛠 Technologies Utilisées

- **Apache Kafka** : Pour le streaming de données en temps réel.
- **Apache Spark** : Pour le traitement et la transformation des données.
- **Apache Airflow** : Pour l'orchestration des tâches du pipeline.
- **Cassandra** : Pour le stockage des données traitées.
- **Redis** : Pour la gestion de l'état et la mise en cache.
- **Docker** : Pour la conteneurisation et le déploiement.
- **Power BI** : Pour la visualisation des données.

---

## 📂 Structure du Projet

Voici la structure du projet :

projet_data_pipeline/  
├── dags/ # DAGs Airflow  
├── dockerfiles/ # Contient les Dockerfile pour Airflow et Spark + le fichier yaml pour Cassandra  
├── images # images et captures du projet  
├── jars # Contient les jars pour Spark, et pour lancer le traitement  
├── scripts/ # Scripts Python (/bashfiles pour lancer le consommateur et la création du topic + producteur Kafka, consommateur Spark)   
├── docker-compose.yml # Configuration Docker pour l'infrastructure  
├── README.md  
└── requirements # Installation pour Python et les autres composants  

---

## 🛠 Installation et Déploiement

### Prérequis

- Docker et Docker Compose installés.
- Power BI Desktop (pour la visualisation).

### Étapes de Déploiement

1. **Cloner le Dépôt** :
   ```bash
   git clone https://github.com/votrenomutilisateur/projet_data_pipeline.git
   cd projet_data_pipeline
   ```
2. **Démarrer les conteneurs Docker**:
    ```bash
    docker-compose up -d
    ```
3. **Créer les Topics Kafka**
    ```bash
    ./scripts/bashfiles/create_topics.sh
    ```
4. **Démarrer le consommateur**
    ```bash
    ./scripts/bashfiles/start_consumer_on_data.sh
    ```
5. **Executer le DAG Airflow**

    * Accédez à l'interface Airflow à l'adresse http://localhost:8082.

    * Déclenchez le DAG kafka_producer_pipeline.

6. **Extraire les données de Cassandra**
    ```bash
    python3 extract_data_from_db.py
    ```
7. **Visualiser les données dans Power BI**

    * Importez le fichier cassandra_data.csv dans Power BI.

    * Créez des tableaux de bord interactifs.


## 🔄 Flux de Données

* Ingestion : Les données sont récupérées à partir d'une API de l'ADEME et envoyées à Kafka.

* Traitement : Spark consomme les données de Kafka, les traite et les stocke dans Cassandra.

* Stockage : Les données traitées sont stockées dans Cassandra.

* Visualisation : Les données sont extraites de Cassandra et visualisées dans Power BI.


## 📊 Visualisation avec Power BI

* Les données traitées sont visualisées dans Power BI pour fournir des insights exploitables. Voici quelques exemples de visualisations :

    * A Remplir

    * A Remplir

    * A remplir



