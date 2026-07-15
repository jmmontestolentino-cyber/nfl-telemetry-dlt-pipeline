import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp, col, from_unixtime, round

# ==============================================================================
# CONFIGURACIÓN DE ORIGEN Y CONTENEDORES (MEDALLION ARCHITECTURE)
# ==============================================================================
catalog_name    = 'nfl_telemetry'
schema_raw_name = 'raw_data'

# Nombres de los Esquemas (Schemas) de destino en Unity Catalog
bronze_schema_name = 'bronze'  
silver_schema_name = 'dev_nfl_telemetry_silver' 
gold_schema_name   = 'dev_nfl_telemetry_gold'

# Nombres de las Tablas Delta
bronze_table_name       = 'nfl_telemetry_v2'
silver_table_name       = 'silver_player_tracking_v2' 
gold_player_table_name  = 'gold_player_realtime_features'
gold_play_table_name    = 'gold_play_trajectory'

# Ruta dinámica del volumen origen de Auto Loader
landing_volume_path = f"/Volumes/{catalog_name}/{schema_raw_name}/landing_data/"


# ==============================================================================
# CAPA BRONZE
# ==============================================================================
@dlt.table(
    name=f"{catalog_name}.{bronze_schema_name}.{bronze_table_name}",
    comment="Tabla Bronze V2 ingiriendo telemetría cruda de la NFL usando Auto Loader",
    table_properties={"quality": "bronze"}
)
def nfl_telemetry_bronze_v2():
    
    df_raw = (
        spark.readStream 
        .format("cloudFiles") 
        .option("cloudFiles.format", "json") 
        .option("cloudFiles.rescuedDataColumn", "_rescued_data") 
        .option("cloudFiles.inferColumnTypes", "true") 
        .load(landing_volume_path)
    )

    df_enriched = (
        df_raw 
        .withColumn("source_file", col("_metadata.file_path")) 
        .withColumn("ingested_at", current_timestamp())
    )

    return df_enriched


# =========================================================================
# CAPA SILVER
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
    
    # 1. LECTURA INTERNA CON FULLY QUALIFIED NAME
    df_raw = dlt.read(f"{catalog_name}.{bronze_schema_name}.{bronze_table_name}")
    
    # 2. DEDUPLICACIÓN
    df_clean = df_raw.dropDuplicates(["game_id", "play_id", "player_id", "timestamp"])
    
    # 3. TRANSFORMACIONES
    df_clean = (
        df_clean 
        .withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp")) 
        .withColumn("speed_mph", col("speed_mph").cast("double")) 
        .withColumn("heart_rate_bpm", col("heart_rate_bpm").cast("int")) 
        .withColumn("acceleration_m_s2", round(col("acceleration_m_s2").cast("double"), 2))
    )
    
    # 4. LIMPIEZA Y AUDITORÍA
    df_clean = df_clean.drop("status", "test", "_rescued_data")
    df_clean = df_clean.withColumn("silver_processed_at", current_timestamp())
    
    return df_clean


# =========================================================================
# CAPA GOLD 1: FEATURES EN TIEMPO REAL POR JUGADOR
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
# CAPA GOLD 2: TRAYECTORIA DE LA JUGADA
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