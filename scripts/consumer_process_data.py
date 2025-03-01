from pyspark.sql import SparkSession
from config import SPARK_MASTER, CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE
from cassandra.cluster import Cluster
import logging
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import param as param
from cassandra.policies import DCAwareRoundRobinPolicy

# =================================================================
# Connexion Spark
# =================================================================
def create_spark_connection():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.kafka:kafka-clients:3.5.1",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
    ]

    try:
        sp_conn = SparkSession.builder \
            .appName("KafkaConsumerAndProcessToDB") \
            .master(SPARK_MASTER) \
            .config("spark.cassandra.connection.host", "172.18.0.5") \
            .config("spark.cassandra.connection.port", "9042") \
            .getOrCreate()
        
        sp_conn.sparkContext.setLogLevel("ERROR")
        print("La connection Spark a été crée avec succès")

    except Exception as e:
        logging.error(f"Spark Session ne peut pas être crée à cause de {e} ")
        sp_conn = None
    
    return sp_conn

# =========================================================
# Connexion Cassandra sans keyspace
# =========================================================
def create_cassandra_connection():

    try:
        cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT),
                           load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'))
        session = cluster.connect()
        print("Connexion à Cassandra établie avec succès")
        return session
    except Exception as e:
        logging.error(f"Erreur lors de la connexion à Cassandra : {e}")
        logging.error("Impossible de se connecter à Cassandra.")
        return None
    
# ==============================================================
# Création du keyspace
# ==============================================================
def create_keyspace(session, keyspace):
    try:
        create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
        """
        session.execute(create_keyspace_query)
        print(f"Keyspace '{keyspace}' vérifié/créé avec succès.")

        # Utiliser le keyspace après sa création
        session.set_keyspace(keyspace)

    except Exception as e:
        print(f"Erreur lors de la création du keyspace : {e}")


# ===============================================
# Création de la table
# ===============================================
def create_table():
    session = create_cassandra_connection()

    if session:
        try:
            create_keyspace(session, CASSANDRA_KEYSPACE)

            session.set_keyspace(CASSANDRA_KEYSPACE)

            create_table_query = """
                CREATE TABLE IF NOT EXISTS mykeyspace.table_projet (
                    code_projet TEXT PRIMARY KEY,
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
                    siren INT,
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
            """
            session.execute(create_table_query)
            print("Table créee avec succès")
        
        except Exception as e:
            print(f"Erreur lors de la création de la table : {e}")
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
    
    return spark_df

# ==========================================================
# Transformation JSON -> DataFrame structuré (Définition du schéma attendu pour les donnees JSON)
# ==========================================================
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
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
        StructField("siren", IntegerType(), True),
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
        StructField("code_epci", StringType(), True)
    ])
    
    # Transformation des messages Kafka (JSON -> DataFrame)
    df_sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    print("Selection dataframe crée avec succès")

    return df_sel

# ==================================================================
# Écriture dans Cassandra
# ==================================================================
def write_in_cassandra(data_df, batch_id):

    print(f"\n Batch {batch_id} : {data_df.count()} lignes recues")

    if data_df.rdd.isEmpty():
        print(f"Aucune donnée reçue pour le batch {batch_id}, on passe.")
        return
    
    try:
        data_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", "table_projet") \
            .option("spark.cassandra.output.ignoreNulls", "true") \
            .mode("append") \
            .save()
        print(f"batch {batch_id} inseré avec succes, {data_df.count()} lignes ajoutées ")
    except Exception as e:
        print(f"Erreur lors de l'écriture dans Cassandra : {e}")
        logging.error(f"Erreur détaillée lors de l'écriture Cassandra: {e}", exc_info=True)

# ======================================================================
# MAIN
# ======================================================================
if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        create_table()
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is not None:
            sel_df = create_selection_df_from_kafka(spark_df)

            print("Début de streaming ...")
            stream_query = sel_df.writeStream \
                .foreachBatch(write_in_cassandra) \
                .outputMode("append") \
                .trigger(processingTime="10 seconds") \
                .start()
            stream_query.awaitTermination()