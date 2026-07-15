import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import col, sqrt, pow, row_number, md5, concat_ws
from pyspark.sql.window import Window

# ==============================================================================
# CONFIGURACIÓN DE ORIGEN Y CONTENEDORES (MEDALLION ARCHITECTURE)
# ==============================================================================
catalog_name       = 'dev_nfl_telemetry_kafka'
silver_schema_name = 'dev_nfl_telemetry_kafka_silver'
gold_schema_name   = 'dev_nfl_telemetry_kafka_gold'

# Nombres de las Tablas Delta
silver_table_name    = 'silver_player_tracking_v1' 
gold_fact_table_name = 'platinum_fact_separation'

# Lectura de parámetros desde la interfaz de Databricks
juego_a_procesar = spark.conf.get("nfl.filtros.game_id", "TODOS")

# =========================================================================
# CAPA GOLD (FACT TABLE): SEPARACIÓN EXACTA CON SURROGATE KEYS
# =========================================================================
@dlt.table(
    name=f"{catalog_name}.{gold_schema_name}.{gold_fact_table_name}",
    comment="Tabla de hechos Gold: Distancia de separación por milisegundo y llaves subrogadas (MD5) para modelo estrella.",
    table_properties={"quality": "gold"}
)
def create_fact_separation():
    
    # 1. Leemos la tabla Silver usando las variables dinámicas de tu estructura
    df_silver = dlt.read(f"{catalog_name}.{silver_schema_name}.{silver_table_name}")
    
    # 2. Aplicamos el filtro del partido si viene parametrizado
    if juego_a_procesar != "TODOS":
        df_silver = df_silver.filter(col("game_id") == juego_a_procesar)
        
    # 3. Separar Ofensiva (Receptores) y Defensiva (Esquineros)
    df_offense = df_silver.filter(col("position") == "WR").alias("off")
    df_defense = df_silver.filter(col("position") == "CB").alias("def")
    
    # 4. Cruce espacial: Todas las distancias posibles en ese instante
    fact_separation_todas = df_offense.join(
        df_defense,
        (col("off.play_id") == col("def.play_id")) & 
        (col("off.timestamp") == col("def.timestamp"))
    ).select(
        col("off.game_id"),
        col("off.play_id"),
        col("off.timestamp"),
        col("off.player_id").alias("off_player_id_raw"),
        col("def.player_id").alias("def_player_id_raw"),
        sqrt(
            pow(col("off.x_coord") - col("def.x_coord"), 2) + 
            pow(col("off.y_coord") - col("def.y_coord"), 2)
        ).alias("separation_yards")
    )
    
    # 5. Lógica de Negocio: Encontrar al defensor más cercano
    window_spec = Window.partitionBy(
        "game_id", 
        "play_id", 
        "timestamp", 
        "off_player_id_raw"
    ).orderBy("separation_yards")
    
    df_closest = fact_separation_todas.withColumn(
        "rank_cercania", row_number().over(window_spec)
    ).filter(
        col("rank_cercania") == 1
    ).drop("rank_cercania")
    
    # 6. GENERACIÓN DE SURROGATE KEYS (HASH MD5)
    df_final = df_closest.withColumn(
        "game_sk", md5(col("game_id"))
    ).withColumn(
        "play_sk", md5(concat_ws("||", col("game_id"), col("play_id")))
    ).withColumn(
        "offensive_player_sk", md5(col("off_player_id_raw"))
    ).withColumn(
        "defensive_player_sk", md5(col("def_player_id_raw"))
    ).drop("off_player_id_raw", "def_player_id_raw")
    
    return df_final