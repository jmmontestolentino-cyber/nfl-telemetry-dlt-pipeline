import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp, col, md5, concat_ws

# ==============================================================================
# CONFIGURACIÓN DE ORIGEN Y CONTENEDORES (MEDALLION ARCHITECTURE)
# ==============================================================================
catalog_name       = 'dev_nfl_telemetry_kafka'

# Nombres de los Esquemas (Schemas) de destino en Unity Catalog
bronze_schema_name = 'dev_nfl_telemetry_kafka_BRONZE'  
silver_schema_name = 'dev_nfl_telemetry_kafka_silver' 
gold_schema_name   = 'dev_nfl_telemetry_kafka_gold'

# Nombres de las Tablas Delta (Dimensiones)
bronze_dim_game_table   = 'bronze_dim_game'
bronze_dim_play_table   = 'bronze_dim_play'
bronze_dim_player_table = 'bronze_dim_player'

silver_dim_game_table   = 'silver_dim_game'
silver_dim_play_table   = 'silver_dim_play'
silver_dim_player_table = 'silver_dim_player'

gold_dim_game_table     = 'gold_dim_game'
gold_dim_play_table     = 'gold_dim_play'
gold_dim_player_table   = 'gold_dim_player'

# Ruta base en tu bucket de AWS S3
bronze_path = "/Volumes/dev_nfl_telemetry_kafka/def_nfl_telemetry_kafka_raw/players_game_plays"

# ==============================================================================
# 1. CAPA BRONZE: INGESTA CRUDA 
# ==============================================================================

@dlt.table(
    name=f"{catalog_name}.{bronze_schema_name}.{bronze_dim_game_table}",
    comment="Capa Bronze: Datos crudos de partidos y estadios desde S3.",
    table_properties={"quality": "bronze"}
)
def create_bronze_dim_game():
    return spark.read.format("csv").option("header", "true").load(f"{bronze_path}/dim_game.csv")


@dlt.table(
    name=f"{catalog_name}.{bronze_schema_name}.{bronze_dim_play_table}",
    comment="Capa Bronze: Datos crudos del contexto de jugadas desde S3.",
    table_properties={"quality": "bronze"}
)
def create_bronze_dim_play():
    return spark.read.format("csv").option("header", "true").load(f"{s3_bronze_path}/dim_play.csv")


@dlt.table(
    name=f"{catalog_name}.{bronze_schema_name}.{bronze_dim_player_table}",
    comment="Capa Bronze: Datos crudos biográficos de jugadores desde S3.",
    table_properties={"quality": "bronze"}
)
def create_bronze_dim_player():
    return spark.read.format("csv").option("header", "true").load(f"{s3_bronze_path}/dim_player.csv")


# ==============================================================================
# 2. CAPA SILVER: LIMPIEZA, CASTING Y GENERACIÓN DE SURROGATE KEYS (MD5)
# ==============================================================================

@dlt.table(
    name=f"{catalog_name}.{silver_schema_name}.{silver_dim_game_table}",
    comment="Capa Silver: Partidos validados con expectativas de calidad y Surrogate Key.",
    table_properties={"quality": "silver"}
)
# Reglas críticas: No podemos procesar un partido sin su ID o sin los equipos
@dlt.expect_or_drop("game_id_valido", "game_id IS NOT NULL AND game_id != ''")
@dlt.expect_or_drop("equipos_validos", "home_team IS NOT NULL AND away_team IS NOT NULL")
# Reglas de monitoreo: Verificar que la temporada y temperatura tengan sentido histórico
@dlt.expect("temporada_coherente", "season >= 2000 AND season <= 2026")
@dlt.expect("temperatura_logica", "temperature_f BETWEEN -20 AND 120")
def create_silver_dim_game():
    return (
        dlt.read(f"{catalog_name}.{bronze_schema_name}.{bronze_dim_game_table}")
        .withColumn("game_sk", md5(col("game_id")))
        .withColumn("season", col("season").cast("int"))
        .withColumn("week", col("week").cast("int"))
        .withColumn("temperature_f", col("temperature_f").cast("int"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )


@dlt.table(
    name=f"{catalog_name}.{silver_schema_name}.{silver_dim_play_table}",
    comment="Capa Silver: Contexto de jugadas validadas con restricciones de la NFL.",
    table_properties={"quality": "silver"}
)
# Reglas críticas: Relación obligatoria con un partido y existencia de la jugada
@dlt.expect_or_drop("llaves_jugada_validas", "play_id IS NOT NULL AND game_id IS NOT NULL")
# Reglas de monitoreo: Validar lógica deportiva de la NFL
@dlt.expect("down_valido", "down BETWEEN 1 AND 4")
@dlt.expect("quarter_valido", "quarter BETWEEN 1 AND 5") # 5 para tiempos extra (Overtime)
@dlt.expect("distancia_positiva", "distance_to_first >= 0")
def create_silver_dim_play():
    return (
        dlt.read(f"{catalog_name}.{bronze_schema_name}.{bronze_dim_play_table}")
        .withColumn("play_sk", md5(concat_ws("||", col("game_id"), col("play_id"))))
        .withColumn("down", col("down").cast("int"))
        .withColumn("distance_to_first", col("distance_to_first").cast("int"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )


@dlt.table(
    name=f"{catalog_name}.{silver_schema_name}.{silver_dim_player_table}",
    comment="Capa Silver: Datos de jugadores limpios y con biometría coherente.",
    table_properties={"quality": "silver"}
)
# 1. Corregimos la expectativa para buscar 'player_sk' en lugar de 'player_id'
@dlt.expect_or_drop("player_id_valido", "player_sk IS NOT NULL AND player_sk != ''")
@dlt.expect_or_drop("nombre_valido", "full_name IS NOT NULL")
@dlt.expect("peso_coherente", "weight_lbs BETWEEN 140 AND 400")
@dlt.expect("estatura_coherente", "height_in BETWEEN 60 AND 90")
def create_silver_dim_player():
    return (
        dlt.read(f"{catalog_name}.{bronze_schema_name}.{bronze_dim_player_table}")
        # 2. Aplicamos el MD5 a la columna que realmente existe en tu CSV y la sobreescribimos
        .withColumn("player_sk", md5(col("player_sk"))) 
        .withColumn("height_in", col("height_in").cast("int"))
        .withColumn("weight_lbs", col("weight_lbs").cast("int"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )


# ==============================================================================
# 3. CAPA GOLD: DIMENSIONES DE NEGOCIO (STAR SCHEMA READY)
# ==============================================================================

@dlt.table(
    name=f"{catalog_name}.{gold_schema_name}.{gold_dim_game_table}",
    comment="Capa Gold: Dimensión oficial de Partidos lista para analítica.",
    table_properties={"quality": "gold"}
)
def create_gold_dim_game():
    return dlt.read(f"{catalog_name}.{silver_schema_name}.{silver_dim_game_table}").select(
        "game_sk",
        "game_id",
        "season",
        "week",
        "home_team",
        "away_team",
        "stadium",
        "turf_type",
        "weather_conditions",
        "temperature_f"
    )


@dlt.table(
    name=f"{catalog_name}.{gold_schema_name}.{gold_dim_play_table}",
    comment="Capa Gold: Dimensión oficial de Jugadas lista para analítica.",
    table_properties={"quality": "gold"}
)
def create_gold_dim_play():
    return dlt.read(f"{catalog_name}.{silver_schema_name}.{silver_dim_play_table}").select(
        "play_sk",
        "play_id",
        "game_id",
        "quarter",
        "down",
        "distance_to_first",
        "expected_coverage",
        "play_result"
    )


@dlt.table(
    name=f"{catalog_name}.{gold_schema_name}.{gold_dim_player_table}",
    comment="Capa Gold: Dimensión oficial de Jugadores lista para analítica.",
    table_properties={"quality": "gold"}
)
def create_gold_dim_player():
    return dlt.read(f"{catalog_name}.{silver_schema_name}.{silver_dim_player_table}").select(
        "player_sk",       # La llave ya procesada con Hash MD5
        "full_name",
        "team",
        "position",
        "height_in",
        "weight_lbs",
        "college",         
        "draft_year"       
    )