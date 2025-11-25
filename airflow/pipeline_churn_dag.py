from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuración por defecto del DAG
default_args = {
    'owner': 'chex',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'entel_churn_prediction_pipeline',
    default_args=default_args,
    description='Pipeline End-to-End: ETL -> Inferencia -> Dashboard',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 10),
    catchup=False,
    tags=['bigdata', 'spark', 'churn'],
) as dag:

    # -----------------------------------------------------------
    # TAREA 1: ETL (Extract, Transform, Load)
    # Ejecuta el script que limpia y vectoriza los datos crudos
    # -----------------------------------------------------------
    task_etl = BashOperator(
        task_id='1_procesar_datos_etl',
        bash_command="""
        export SPARK_HOME=/opt/spark-3.5.1-bin-hadoop3 && \
        $SPARK_HOME/bin/spark-submit --driver-memory 4g --executor-memory 4g /home/chex/BigData_UPAO/airflow_env/scripts/etl_churn.py
        """
    )

    # -----------------------------------------------------------
    # TAREA 2: INFERENCIA (Machine Learning)
    # Ejecuta el script que aplica el modelo XGBoost
    # -----------------------------------------------------------
    task_inference = BashOperator(
        task_id='2_predecir_churn',
        bash_command="""
        export SPARK_HOME=/opt/spark-3.5.1-bin-hadoop3 && \
        $SPARK_HOME/bin/spark-submit --driver-memory 2g --executor-memory 2g /home/chex/BigData_UPAO/airflow_env/scripts/inference_churn.py
        """
    )

    # -----------------------------------------------------------
    # TAREA 3: NOTIFICACIÓN FINAL
    # Simplemente imprime un mensaje de éxito en los logs
    # -----------------------------------------------------------
    task_notify = BashOperator(
        task_id='3_pipeline_finalizado',
        bash_command='echo "Pipeline completado. Dashboard actualizado en: http://localhost:8501"'
    )

    # -----------------------------------------------------------
    # DEFINICIÓN DE DEPENDENCIAS (ORDEN DE EJECUCIÓN)
    # 1 -> 2 -> 3
    # -----------------------------------------------------------
    task_etl >> task_inference >> task_notify

