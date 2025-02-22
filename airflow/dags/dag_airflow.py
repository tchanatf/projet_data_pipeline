from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor
from datetime import datetime, timedelta
import psycopg2

# Paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_spark_postgres_pipeline',
    default_args=default_args,
    description='Pipeline Kafka-Spark-Postgres orchestré par Airflow',
    schedule_interval='@daily',  # Peut être modifié selon les besoins
    catchup=False,
)

# Vérifier que Kafka est en ligne
check_kafka = BashSensor(
    task_id='check_kafka',
    bash_command='nc -z -v -w5 localhost 29092',
    timeout=60,
    mode='poke',
    dag=dag,
)

# Création du topic Kafka
create_kafka_topic = BashOperator(
    task_id='create_kafka_topic',
    bash_command="docker exec -it kafka kafka-topics --create --topic apiRealTimeAndFileData --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1 || true",
    dag=dag,
)

# Lancer le producteur Kafka
start_producer = BashOperator(
    task_id='start_producer',
    bash_command='python3 /app/scripts/producer_kafka_data.py',
    dag=dag,
)

# Lancer le consommateur Spark
start_consumer = SparkSubmitOperator(
    task_id='start_consumer',
    application='/app/scripts/consumer_kafka_and_process_data.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

# Vérifier que les données sont bien insérées dans PostgreSQL
def verify_postgres():
    conn = psycopg2.connect(
        dbname="your_database",
        user="your_username",
        password="your_password",
        host="localhost"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM table_data;")
    result = cur.fetchone()
    print(f"Nombre de lignes dans table_data: {result[0]}")
    cur.close()
    conn.close()

# Tâche pour vérifier les données dans PostgreSQL
verify_postgres_task = PythonOperator(
    task_id='verify_postgres',
    python_callable=verify_postgres,
    dag=dag,
)

# Définition de l'ordre des tâches
check_kafka >> create_kafka_topic >> start_producer >> start_consumer >> verify_postgres_task