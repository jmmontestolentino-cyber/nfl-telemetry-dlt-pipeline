import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp, col, from_unixtime, round, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

# ==============================================================================
# CONFIGURACIÓN DE ORIGEN Y CONTENEDORES (MEDALLION ARCHITECTURE)
# ==============================================================================
catalog_name    = 'dev_nfl_telemetry_kafka'

# Nombres de los Esquemas (Schemas) de destino en Unity Catalog
bronze_schema_name = 'dev_nfl_telemetry_kafka_0'  
silver_schema_name = 'dev_nfl_telemetry_kafka_silver' 
gold_schema_name   = 'dev_nfl_telemetry_kafka_gold'

# Nombres de las Tablas Delta
bronze_table_name       = 'nfl_telemetry_v1'
silver_table_name       = 'silver_player_tracking_v1' 
gold_player_table_name  = 'gold_player_realtime_features'
gold_play_table_name    = 'gold_play_trajectory'

# ==============================================================================
# CONFIGURACIÓN DE CONFLUENT CLOUD (KAFKA)
# ==============================================================================
# NOTA DE PRODUCCIÓN: En un entorno real, las contraseñas nunca van en texto plano.
# Se recomienda usar Databricks Secrets: dbutils.secrets.get(scope="confluent", key="api_key")

confluent_bootstrap_servers = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
confluent_topic_name        = "nfl_telemetry"
confluent_api_key           = "HZREEIU2DXNEDHJ4"
confluent_api_secret        = "cflt2/j0qPgv0Ptaz3bEbYcIG0sxk/EKixzYl9wBN5l1smJ74BQk3tMLBXiWIaLw"

# Definición del esquema JSON esperado desde el tópico de Kafka
# Definición del esquema JSON esperado desde el tópico de Kafka
telemetry_schema = StructType([
    StructField("game_id", StringType(), True),
    StructField("play_id", StringType(), True),
    StructField("play_status", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("position", StringType(), True),      # <-- ¡El rol del jugador (WR, CB)!
    StructField("team", StringType(), True),
    StructField("x_coord", DoubleType(), True),       # <-- Coordenada X
    StructField("y_coord", DoubleType(), True),       # <-- Coordenada Y
    StructField("speed_mph", DoubleType(), True),
    StructField("acceleration_m_s2", DoubleType(), True),
    StructField("heart_rate_bpm", IntegerType(), True),
    StructField("stamina_pct", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Cadena de conexión segura JAAS para Confluent
jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{confluent_api_key}' password='{confluent_api_secret}';"

# ==============================================================================
# CAPA BRONZE (STREAMING DESDE KAFKA/CONFLUENT)
# ==============================================================================
@dlt.table(
    name=f"{catalog_name}.{bronze_schema_name}.{bronze_table_name}",
    comment="Tabla Bronze V2 ingiriendo telemetría cruda en vivo desde Confluent Cloud (Kafka)",
    table_properties={"quality": "bronze"}
)
def nfl_telemetry_bronze_v2():
    
    # 1. Conexión al Tópico de Kafka
    df_raw = (
        spark.readStream 
        .format("kafka") 
        .option("kafka.bootstrap.servers", confluent_bootstrap_servers) 
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("subscribe", confluent_topic_name) 
        .option("startingOffsets", "latest") # O 'latest' si solo quieres datos desde que inicia el pipeline
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Decodificación del Payload (De Binario a JSON a Columnas)
    df_enriched = (
        df_raw 
        # Kafka trae el mensaje en la columna 'value' como binario. Lo pasamos a string y luego a JSON.
        .withColumn("json_payload", from_json(col("value").cast("string"), telemetry_schema)) 
        # Expandimos el JSON en columnas nativas (json_payload.*)
        .select("json_payload.*", col("timestamp").alias("kafka_timestamp"), "topic", "partition", "offset") 
        .withColumn("ingested_at", current_timestamp())
    )

    return df_enriched


# =========================================================================
# CAPA SILVER (INTACTA)
# =========================================================================
@dlt.table(
    name=f"{catalog_name}.{silver_schema_name}.{silver_table_name}", 
    comment="Capa Silver de telemetría NFL: Datos limpios, tipados y deduplicados",
    partition_cols=["game_id"],
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("velocidad_valida", "speed_mph >= 0.0 AND speed_mph <= 30.0")
@dlt.expect_or_drop("ritmo_cardiaco_valido", "heart_rate_bpm IS NOT NULL")
def create_silver_player_tracking():
    
    df_raw = dlt.read(f"{catalog_name}.{bronze_schema_name}.{bronze_table_name}")
    
    df_clean = df_raw.dropDuplicates(["game_id", "play_id", "player_id", "timestamp"])
    
    df_clean = (
        df_clean 
        .withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp")) 
        .withColumn("speed_mph", col("speed_mph").cast("double")) 
        .withColumn("heart_rate_bpm", col("heart_rate_bpm").cast("int")) 
        .withColumn("acceleration_m_s2", round(col("acceleration_m_s2").cast("double"), 2))
    )
    
    # Eliminamos las columnas técnicas de Kafka para dejar el dato de negocio limpio
    df_clean = df_clean.drop("topic", "partition", "offset", "kafka_timestamp")
    df_clean = df_clean.withColumn("silver_processed_at", current_timestamp())
    
    return df_clean


# =========================================================================
# CAPA GOLD 1: FEATURES EN TIEMPO REAL POR JUGADOR (INTACTA)
# =========================================================================
@dlt.table(
    name=f"{catalog_name}.{gold_schema_name}.{gold_player_table_name}",
    comment="One Big Table (OBT) optimizada para ML."
)
def gold_player_realtime_features():
    
    df_silver = dlt.read(f"{catalog_name}.{silver_schema_name}.{silver_table_name}")
    
    df_gold = df_silver.groupBy("player_id", "game_id").agg(
        F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
        F.max("speed_mph").alias("max_speed_today"),
        F.round(F.avg("heart_rate_bpm"), 0).alias("avg_heart_rate"),
        F.max("heart_rate_bpm").alias("peak_heart_rate"),
        
        F.collect_list("acceleration_m_s2").alias("acceleration_history_array"),
        
        F.current_timestamp().alias("feature_timestamp")
    )
    
    return df_gold


# =========================================================================
# CAPA GOLD 2: TRAYECTORIA DE LA JUGADA (INTACTA)
# =========================================================================
@dlt.table(
    name=f"{catalog_name}.{gold_schema_name}.{gold_play_table_name}",
    comment="OBT para predecir el tipo de jugada. Agrupa la telemetría a nivel de Jugada (Play)."
)
def create_gold_play_trajectory():
    
    df_silver = dlt.read(f"{catalog_name}.{silver_schema_name}.{silver_table_name}")
    
    df_gold_play = df_silver.groupBy("game_id", "play_id").agg(
        F.collect_list(
            F.struct("player_id", "timestamp", "speed_mph", "acceleration_m_s2")
        ).alias("telemetry_history_array"),
        
        F.round(F.max("speed_mph"), 2).alias("max_speed_in_play"),
        F.countDistinct("player_id").alias("players_tracked"),
        
        F.current_timestamp().alias("feature_timestamp")
    )
    
    return df_gold_play