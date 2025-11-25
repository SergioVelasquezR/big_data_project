from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType, BooleanType
)
from pyspark.sql.functions import col, count, sum, avg, lit, current_date, datediff, when, coalesce
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

def iniciar_spark():
    """Inicia la sesión de Spark."""
    return SparkSession.builder \
        .appName("ETL_Churn_Automated") \
        .getOrCreate()

def extraer_datos(spark):
    """
    FASE 1: EXTRACCIÓN
    Carga los datos crudos desde HDFS usando los esquemas definidos.
    """
    print(">>> INICIANDO FASE DE EXTRACCIÓN...")

    # --- 1. DEFINICIÓN DE ESQUEMAS ---
    schema_llamadas = StructType([
        StructField("id_registro", StringType(), True),
        StructField("id_cliente", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("duracion_segundos", DoubleType(), True),
        StructField("datos_mb_consumidos", DoubleType(), True),
        StructField("calidad_red", IntegerType(), True)
    ])

    schema_facturacion = StructType([
        StructField("id_factura", StringType(), True),
        StructField("id_cliente", StringType(), True),
        StructField("fecha_emision", DateType(), True),
        StructField("monto", DoubleType(), True),
        StructField("estado_pago", StringType(), True),
        StructField("reclamo_presentado", BooleanType(), True)
    ])

    schema_social_media = StructType([
        StructField("id_comentario", StringType(), True),
        StructField("id_cliente", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("red_social", StringType(), True),
        StructField("texto_comentario", StringType(), True)
    ])

    schema_clients = StructType([
        StructField("id_cliente", StringType(), True),
        StructField("nombre_completo", StringType(), True),
        StructField("fecha_alta", DateType(), True),
        StructField("id_plan", StringType(), True),
        StructField("region", StringType(), True),
        StructField("churn_futuro", BooleanType(), True)
    ])

    # --- 2. RUTAS HDFS ---
    base_path = "hdfs://localhost:9000/chex/BigData_UPAO/bigdata_env/churn_project/raw"
    path_llamadas = f"{base_path}/json/llamadas"
    path_facturacion = f"{base_path}/json/facturacion"
    path_social_media = f"{base_path}/parquet/social_media"
    path_clients = f"{base_path}/csv/clients"

    # --- 3. LECTURA ---
    df_llamadas = spark.read.option("multiline", "true").schema(schema_llamadas).json(path_llamadas)
    df_facturacion = spark.read.option("multiline", "true").schema(schema_facturacion).json(path_facturacion)
    df_social_media = spark.read.schema(schema_social_media).parquet(path_social_media)
    df_clients = spark.read.csv(path_clients, header=True, schema=schema_clients, dateFormat="yyyy-MM-dd")

    print(">>> EXTRACCIÓN COMPLETADA.")
    return df_llamadas, df_facturacion, df_social_media, df_clients

def transformar_datos(df_llamadas, df_facturacion, df_social_media, df_clients):
    """
    FASE 2: TRANSFORMACIÓN
    Agrega, limpia, hace joins y vectoriza los datos.
    """
    print(">>> INICIANDO FASE DE TRANSFORMACIÓN...")

    # --- A. AGREGACIONES (Feature Engineering) ---
    # 1. Resumen de Llamadas
    agg_llamadas = df_llamadas.groupBy("id_cliente").agg(
        sum("duracion_segundos").alias("total_segundos_llamada"),
        sum("datos_mb_consumidos").alias("total_mb_consumidos"),
        avg("calidad_red").alias("promedio_calidad_red"),
        count("id_registro").alias("total_llamadas")
    )

    # 2. Resumen de Facturación
    agg_facturacion = df_facturacion.groupBy("id_cliente").agg(
        count("id_factura").alias("total_facturas"),
        sum("monto").alias("monto_total_facturado"),
        sum(when(col("estado_pago") == "Pendiente", 1).otherwise(0)).alias("total_facturas_pendientes"),
        sum(when(col("reclamo_presentado") == True, 1).otherwise(0)).alias("total_reclamos")
    )

    # 3. Resumen Social Media
    agg_social = df_social_media.groupBy("id_cliente").agg(
        count("id_comentario").alias("total_comentarios_social")
    )

    # --- B. UNIFICACIÓN (JOINS) ---
    df_master = df_clients.join(agg_llamadas, "id_cliente", "left") \
                          .join(agg_facturacion, "id_cliente", "left") \
                          .join(agg_social, "id_cliente", "left")

    # --- C. LIMPIEZA DE NULOS ---
    df_master = df_master.fillna(0) # Los nulos numéricos se vuelven 0
    
    # Crear variable antiguedad
    df_master = df_master.withColumn("antiguedad_dias", datediff(current_date(), col("fecha_alta")))

    # --- D. VECTORIZACIÓN (ML PREP) ---
    # Identificamos columnas
    categorical_cols = ["id_plan", "region"]
    numeric_cols = [
        "antiguedad_dias", "total_llamadas", "total_segundos_llamada", 
        "total_mb_consumidos", "promedio_calidad_red", "total_facturas", 
        "monto_total_facturado", "total_reclamos", 
        "total_facturas_pendientes", "total_comentarios_social"
    ]

    stages = []
    
    # Indexar y Codificar Categorías
    for c in categorical_cols:
        indexer = StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep")
        encoder = OneHotEncoder(inputCols=[indexer.getOutputCol()], outputCols=[f"{c}_vec"])
        stages += [indexer, encoder]

    # Ensamblar Vector Final
    assembler_inputs = numeric_cols + [f"{c}_vec" for c in categorical_cols]
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid="keep")
    stages += [assembler]

    # Ejecutar Pipeline
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(df_master)
    df_final = model.transform(df_master)

    print(">>> TRANSFORMACIÓN COMPLETADA.")
    return df_final

def cargar_datos(df_final):
    """
    FASE 3: CARGA (LOAD)
    Guarda el resultado procesado en HDFS.
    """
    print(">>> INICIANDO FASE DE CARGA...")
    
    # Ruta destino (Zona Procesada)
    # OJO: Usamos la subcarpeta 'model_features_full' para evitar errores de lectura
    output_path = "hdfs://localhost:9000/chex/BigData_UPAO/bigdata_env/churn_project/processed"
    
    df_final.write.mode("overwrite").parquet(output_path)
    
    print(f">>> CARGA COMPLETADA. Archivo guardado en: {output_path}")

# --- BLOQUE PRINCIPAL (ENTRY POINT) ---
if __name__ == "__main__":
    spark = iniciar_spark()
    
    try:
        # 1. Extraer
        df_l, df_f, df_s, df_c = extraer_datos(spark)
        
        # 2. Transformar
        df_procesado = transformar_datos(df_l, df_f, df_s, df_c)
        
        # 3. Cargar
        cargar_datos(df_procesado)
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO EN EL PIPELINE: {e}")
        spark.stop()
        raise e
        
    spark.stop()
