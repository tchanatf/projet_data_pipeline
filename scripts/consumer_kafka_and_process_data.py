from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DataType, DecimalType, NumericType
import param as param
import psycopg2
from pyspark.sql.streaming import StreamingQueryException
import signal
import sys


# Création d'une SparkSession avec le master pointant sur le cluster Spark.
def create_spark_connection():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.2"
    ]

    sp_conn = None
    try:
        sp_conn = SparkSession.builder \
            .appName("KafkaConsumerAndProcessToDB") \
            .master("local[*]") \
            .config("spark.jars.packages", ",".join(packages)) \
            .getOrCreate()
        
        sp_conn.sparkContext.setLogLevel("ERROR")
        print("La connection Spark a été crée avec succès")

    except Exception as e:
        logging.error(f"Spark Session ne peut pas être crée à cause de {e} ")
    
    return sp_conn

# Connexion à postgre via jdbc
def create_postgre_conn():        
    jdbc_url = f"jdbc:postgresql://localhost:5432/{param.DB_NAME}"
    properties = {
        "user": param.DB_USER,
        "password": param.DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    return jdbc_url, properties

# Fonction pour créer la table si elle n'existe pas
def create_table():
    create_table_query = """
                CREATE TABLE IF NOT EXISTS table_data (
                    Code_projet TEXT,
                    Libellé_projet TEXT,
                    Type_de_projet TEXT,
                    Action TEXT,
                    Programme TEXT,
                    Volet TEXT,
                    Thématique TEXT,
                    Appel_à_projet TEXT,
                    Date_de_dépôt_d_un_dossier_complet TEXT,
                    Date_de_décision_initiale_PM TEXT,
                    Date_de_notification TEXT,
                    Description_courte TEXT,
                    Durée_du_projet_mois INTEGER,
                    Acteur TEXT,
                    SIRET TEXT,
                    SIREN INTEGER,
                    Montants_autorisés_Subventions NUMERIC,
                    Montants_autorisés_AR NUMERIC,
                    Total_autorisé NUMERIC,
                    Total_autorisé_avant_indus NUMERIC,
                    Total_contractualisé NUMERIC,
                    Total_contractualisé_avant_indus NUMERIC,
                    Total_indus NUMERIC,
                    Type_Etablissement TEXT,
                    Code_INSEE_Etablissement TEXT,
                    Libelle_Commune_Etablissement TEXT,
                    Code_NAF TEXT,
                    Longitude_Commune_Etablissement DOUBLE PRECISION,
                    Latitude_Commune_Etablissement DOUBLE PRECISION,
                    Code_Département TEXT,
                    Libellé_Département TEXT,
                    Code_Région INTEGER,
                    Libellé_Région TEXT,
                    Code_EPCI TEXT
                );
            """
    conn = None
    cur = None 
    try:
        conn = psycopg2.connect(
            host = param.DB_HOST,
            port = param.DB_PORT,
            database = param.DB_NAME,
            user = param.DB_USER,
            password = param.DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        print("Table créee avec succès")

    except Exception as e:
        print(f"Erreur lors de la création de la table : {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Lecture du flux de données depuis Kafka.
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:29092") \
            .option("subscribe", param.KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()
        print("Kafka df est crée")
    
    except Exception as e:
        print(f"Kafka df ne peut pas etre crée du a {e}")
    
    return spark_df

    
def create_selection_df_from_kafka(spark_df):
    # Définition du schéma attendu pour les données JSON.
    schema = StructType([
        StructField("Code_projet", StringType(), True),
        StructField("Libellé_projet", StringType(), True),
        StructField("Type_de_projet", StringType(), True),
        StructField("Action", StringType(), True),
        StructField("Programme", StringType(), True),
        StructField("Volet", StringType(), True),
        StructField("Thématique", StringType(), True),
        StructField("Appel_à_projet", StringType(), True),
        StructField("Date_de_dépôt_d_un_dossier_complet", StringType(), True),
        StructField("Date_de_décision_initiale_PM", StringType(), True),
        StructField("Date_de_notification", StringType(), True),
        StructField("Description_courte", StringType(), True),
        StructField("Durée_du_projet_mois", IntegerType(), True),
        StructField("Acteur", StringType(), True),
        StructField("SIRET", StringType(), True),
        StructField("SIREN", IntegerType(), True),
        StructField("Montants_autorisés_Subventions", DoubleType(), True),
        StructField("Montants_autorisés_AR", DoubleType(), True),
        StructField("Total_autorisé", DoubleType(), True),
        StructField("Total_autorisé_avant_indus", DoubleType(), True),
        StructField("Total_contractualisé", DoubleType(), True),
        StructField("Total_contractualisé_avant_indus", DoubleType(), True),
        StructField("Total_indus", DoubleType(), True),
        StructField("Type_Etablissement", StringType(), True),
        StructField("Code_INSEE_Etablissement", StringType(), True),
        StructField("Libelle_Commune_Etablissement", StringType(), True),
        StructField("Code_NAF", StringType(), True),
        StructField("Longitude_Commune_Etablissement", DoubleType(), True),
        StructField("Latitude_Commune_Etablissement", DoubleType(), True),
        StructField("Code_Département", StringType(), True),
        StructField("Libellé_Département", StringType(), True),
        StructField("Code_Région", IntegerType(), True),
        StructField("Libellé_Région", StringType(), True),
        StructField("Code_EPCI", StringType(), True)
    ])
    
    # Transformation des messages Kafka (JSON -> DataFrame)
    df_sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    print("Selection dataframe crée avec succès")
  
    return df_sel

# Ecrire dans Postgre
def write_in_postgre(data_df, data_id):
    # # Debug: afficher un échantillon des données traitées
    # print("Exemple de données à insérer:")
    # data_df.show(1, truncate=False)
    jdbc_url, properties = create_postgre_conn()

    try:
        if not data_df.isEmpty():
            data_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "table_data") \
                .option("user", properties["user"]) \
                .option("password", properties["password"]) \
                .option("driver", properties["driver"]) \
                .mode("append") \
                .save()
            print(f"batch {data_id} inseré avec succes")
            print(f"Écriture de {data_df.count()} lignes")

    except Exception as e:
        print(f"Erreur lors de l'écriture dans PostgreSQL : {e}")

# Gestion de l'arret du stream et de spark
def signal_handler(sig, frame):
    print("Arrêt du streaming...")
    try:
        if stream_query and stream_query.isActive:
            stream_query.stop()
            print("Streaming arrêté avec succès.")
    except StreamingQueryException as e:
        print(f"Erreur lors de l'arrêt du streaming : {e}")
    finally:
        spark_conn.stop()
        sys.exit(0)

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        create_table()
        spark_df = connect_to_kafka(spark_conn)
        sel_df = create_selection_df_from_kafka(spark_df)

        print("Debut de streaming ...")
        stream_query = sel_df.writeStream \
                            .foreachBatch(write_in_postgre) \
                            .outputMode("append") \
                            .start()
        
        # Gestion de l'interruption
        signal.signal(signal.SIGINT, signal_handler)

        stream_query.awaitTermination()
        
        # try:
        #     stream_query.awaitTermination()

        # except KeyboardInterrupt:
        #     print("\nInterruption détectée ! Arrêt propre du streaming...")
        #     stream_query.stop()
        #     spark_conn.stop()
        #     print("Streaming de données arrêté proprement")