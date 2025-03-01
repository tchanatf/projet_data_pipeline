from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from datetime import datetime

# Paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 1
}

dag = DAG(
    'kafka_producer_pipeline',
    default_args=default_args,
    description='Pipeline Kafka Producer orchestré par Airflow',
    schedule_interval=None,
    catchup=False,
)

# Vérifier que Kafka est en ligne
check_kafka = BashSensor(
    task_id='check_kafka',
    bash_command='nc -z -v -w5 {{ var.value.KAFKA_HOST }} {{ var.value.KAFKA_PORT }}',
    env={
        'KAFKA_HOST': 'kafka',
        'KAFKA_PORT': '9092',
    },
    timeout=300,
    mode="reschedule",  
    dag=dag,
)

# Lancer le producteur Kafka
start_producer = BashOperator(
    task_id='start_producer',
    bash_command='nohup python3 /app/scripts/producer_kafka_data.py > /dev/null 2>&1 & echo $! > /tmp/producer_kafka.pid',
    # bash_command='python3 /app/scripts/producer_kafka_data.py',
    dag=dag,
)

# Définition de l'ordre des tâches
check_kafka >> start_producer