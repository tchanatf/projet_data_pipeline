from pyspark.sql import SparkSession
from config import SPARK_MASTER, CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE, CASSANDRA_LOCAL_DC
from cassandra.cluster import Cluster
from pyspark.sql.functions import expr
import uuid
import logging
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import param as param
from cassandra.policies import DCAwareRoundRobinPolicy
import signal
import sys
import time


# Configuration des logs
logging.basicConfig(level=logging.INFO)

# =================================================================
# Gestionnaire de signal pour arrêter proprement le streaming
# =================================================================
def signal_handler(sig, frame):
    print("\nInterruption détectée. Arrêt propre du streaming...")
    if 'stream_query' in globals():
        stream_query.stop()
    sys.exit(0)

# Enregistrer le gestionnaire de signal pour SIGINT (Ctrl + C)
signal.signal(signal.SIGINT, signal_handler)

# =================================================================
# Connexion Spark
# =================================================================
def create_spark_connection():
    try:
        sp_conn = SparkSession.builder \
            .appName("KafkaConsumerAndProcessToDB") \
            .master(SPARK_MASTER) \
            .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
            .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
            .getOrCreate()
        
        sp_conn.sparkContext.setLogLevel("ERROR")
        print("La connection Spark établie avec succès")
        return sp_conn
    except Exception as e:
        logging.error(f"Erreur création Spark Session : {e} ")
        return None
    
    return sp_conn

# =========================================================
# Connexion Cassandra
# =========================================================
def create_cassandra_connection():
    try:
        cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT),
                           load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=CASSANDRA_LOCAL_DC))
        session = cluster.connect()
        print("Connexion à Cassandra OK")
        return session
    except Exception as e:
        logging.error(f"Erreur connexion Cassandra : {e}")
        return None
    
# ==============================================================
# Création du Keyspace & Table
# ==============================================================

def create_keyspace_and_table():
    session = create_cassandra_connection()
    if session:
        try:
            # Keyspace
            session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
            """)
            print(f"Keyspace '{CASSANDRA_KEYSPACE}' créé.")

            session.set_keyspace(CASSANDRA_KEYSPACE)

            # Table
            session.execute("""
            CREATE TABLE IF NOT EXISTS table_projet_avenir (
                uuid UUID PRIMARY KEY,
                code_projet TEXT,
                libelle_projet TEXT,
                type_de_projet TEXT,
                action TEXT,
                programme TEXT,
                volet TEXT,
                thematique TEXT,
                appel_a_projet TEXT,
                date_de_depot_dun_dossier_complet TEXT,
                date_de_decision_initiale_pm TEXT,
                date_de_notification TEXT,
                description_courte TEXT,
                duree_du_projet_mois INT,
                acteur TEXT,
                siret TEXT,
                siren TEXT,
                montants_autorises_subventions DOUBLE,
                montants_autorises_ar DOUBLE,
                total_autorise DOUBLE,
                total_autorise_avant_indus DOUBLE,
                total_contractualise DOUBLE,
                total_contractualise_avant_indus DOUBLE,
                total_indus DOUBLE,
                type_etablissement TEXT,
                code_insee_etablissement TEXT,
                libelle_commune_etablissement TEXT,
                code_naf TEXT,
                longitude_commune_etablissement DOUBLE,
                latitude_commune_etablissement DOUBLE,
                code_departement TEXT,
                libelle_departement TEXT,
                code_region INT,
                libelle_region TEXT,
                code_epci TEXT
            );
            """)
            print("Table 'table_projet_avenir' créée.")
        except Exception as e:
            logging.error(f"Erreur création keyspace/table : {e}")
        finally:
            session.shutdown()

# ===============================================================
# Connexion Kafka
# ===============================================================
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "apiRealTimeAndFileData") \
            .option("startingOffsets", "earliest") \
            .load()
        print("Kafka df est crée")
        return spark_df
    except Exception as e:
        print(f"Kafka df ne peut pas etre crée du a {e}")
        return None

# ===============================================
# SCHEMA JSON
# ===============================================
def get_schema():
    return StructType([
        StructField("code_projet", StringType(), True),
        StructField("libelle_projet", StringType(), True),
        StructField("type_de_projet", StringType(), True),
        StructField("action", StringType(), True),
        StructField("programme", StringType(), True),
        StructField("volet", StringType(), True),
        StructField("thematique", StringType(), True),
        StructField("appel_a_projet", StringType(), True),
        StructField("date_de_depot_dun_dossier_complet", StringType(), True),
        StructField("date_de_decision_initiale_pm", StringType(), True),
        StructField("date_de_notification", StringType(), True),
        StructField("description_courte", StringType(), True),
        StructField("duree_du_projet_mois", IntegerType(), True),
        StructField("acteur", StringType(), True),
        StructField("siret", StringType(), True),
        StructField("siren", StringType(), True),
        StructField("montants_autorises_subventions", DoubleType(), True),
        StructField("montants_autorises_ar", DoubleType(), True),
        StructField("total_autorise", DoubleType(), True),
        StructField("total_autorise_avant_indus", DoubleType(), True),
        StructField("total_contractualise", DoubleType(), True),
        StructField("total_contractualise_avant_indus", DoubleType(), True),
        StructField("total_indus", DoubleType(), True),
        StructField("type_etablissement", StringType(), True),
        StructField("code_insee_etablissement", StringType(), True),
        StructField("libelle_commune_etablissement", StringType(), True),
        StructField("code_naf", StringType(), True),
        StructField("longitude_commune_etablissement", DoubleType(), True),
        StructField("latitude_commune_etablissement", DoubleType(), True),
        StructField("code_departement", StringType(), True),
        StructField("libelle_departement", StringType(), True),
        StructField("code_region", IntegerType(), True),
        StructField("libelle_region", StringType(), True),
        StructField("code_epci", StringType(), True),
        StructField("uuid", StringType(), False) 
    ])

# ==========================================================
# Transformation JSON -> DF Structuré
# ==========================================================
def create_selection_df_from_kafka(spark_df):
    schema = get_schema()
    
    # Transformation des données Kafka 
    df_sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("uuid", expr("uuid()"))
    
    print("Dataframe structuré prêt.")
    return df_sel

# ==================================================================
# Écriture dans Cassandra
# ==================================================================
def write_in_cassandra(data_df, batch_id):
    try:
        if data_df.rdd.isEmpty():
            print(f"Batch {batch_id}: vide, on passe.")
            return
        # Tentative d'écriture avec retry simple
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                data_df.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .option("keyspace", CASSANDRA_KEYSPACE) \
                    .option("table", "table_projet_avenir") \
                    .option("spark.cassandra.output.ignoreNulls", "true") \
                    .mode("append") \
                    .save()
                print(f"Batch {batch_id} inséré avec succès.")
                break
            except Exception as e:
                print(f"Erreur Cassandra (tentative {attempt}/{max_retries}) : {e}")
                time.sleep(5)  # attendre avant retry
        else:
            logging.error(f"Échec écriture batch {batch_id} après {max_retries} tentatives.")
    except Exception as e:
        logging.error(f"Erreur batch {batch_id}: {e}", exc_info=True)
# ======================================================================
# MAIN
# ======================================================================
if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn:
        # Création keyspace/table séparée (à faire qu'une fois si possible)
        create_keyspace_and_table()

        kafka_df = connect_to_kafka(spark_conn)

        if kafka_df:
            sel_df = create_selection_df_from_kafka(kafka_df)

            print("Début du streaming ...")
            stream_query = sel_df.writeStream \
                .foreachBatch(write_in_cassandra) \
                .outputMode("append") \
                .trigger(processingTime="10 seconds") \
                .start()

            stream_query.awaitTermination()