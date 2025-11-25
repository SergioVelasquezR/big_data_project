from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col
import sys

def iniciar_spark():
    return SparkSession.builder \
        .appName("Inferencia_Churn_Automated") \
        .getOrCreate()

def cargar_datos_y_modelo(spark):
    """Carga los datos procesados del ETL y el modelo entrenado."""
    print(">>> CARGANDO DATOS Y MODELO...")
    
    # 1. Datos procesados (Salida del ETL anterior)
    ruta_datos = "hdfs://localhost:9000/chex/BigData_UPAO/bigdata_env/churn_project/processed"
    
    # 2. Modelo Entrenado (Ajusta 'modelo_xgboost_v1' si tu carpeta se llama diferente)
    ruta_modelo = "/home/chex/BigData_UPAO/bigdata_env/modelo_churn/"
    
    try:
        df_input = spark.read.parquet(ruta_datos)
        model = GBTClassificationModel.load(ruta_modelo)
        print(">>> CARGA EXITOSA.")
        return df_input, model
    except Exception as e:
        print(f"Error cargando recursos: {e}")
        sys.exit(1) # Forzar error para que Airflow se entere

def generar_predicciones(df_input, model):
    """Aplica el modelo y limpia la salida."""
    print(">>> EJECUTANDO PREDICCIONES...")
    
    # Transformación (Inferencia)
    df_predicciones = model.transform(df_input)
    
    # Selección de columnas de negocio
    df_final = df_predicciones.select(
        "id_cliente", 
        "region", 
        "id_plan", 
        "antiguedad_dias", 
        "monto_total_facturado",
        "total_facturas_pendientes", 
        "promedio_calidad_red", 
        "total_comentarios_social",
        "probability", 
        "prediction"
    )
    
    # Extraer probabilidad limpia (Vector -> Double)
    # Tomamos el índice 1 que corresponde a la probabilidad de "Sí Churn"
    df_final = df_final.withColumn(
        "prob_churn", 
        vector_to_array(col("probability"))[1]
    ).drop("probability")
    
    print(">>> INFERENCIA COMPLETADA.")
    return df_final

def guardar_resultados(df_final):
    """Guarda el resultado final para el Dashboard y NiFi."""
    print(">>> GUARDANDO RESULTADOS...")
    
    # Ruta local para Streamlit/NiFi
    ruta_salida = "/home/chex/BigData_UPAO/output_modelo"
    
    df_final.write.mode("overwrite").parquet(ruta_salida)
    print(f">>> GUARDADO EN: {ruta_salida}")

if __name__ == "__main__":
    spark = iniciar_spark()
    
    df, modelo = cargar_datos_y_modelo(spark)
    df_predicho = generar_predicciones(df, modelo)
    guardar_resultados(df_predicho)
    
    spark.stop()
