# 🏈 Pipeline de Telemetría de la NFL: Streaming y Batch Unificado vía Delta Live Tables (DLT)

## 📌 Resumen General
Este repositorio demuestra un pipeline de Ingeniería de Datos de extremo a extremo que procesa datos de telemetría de jugadores de la NFL en tiempo real. Simula métricas de seguimiento espacial de alta frecuencia, las transmite a través de Confluent Kafka y procesa los datos mediante una estricta Arquitectura Medallón (Bronce, Plata, Oro) utilizando Databricks Delta Live Tables (DLT) y PySpark.

El pipeline culmina en activos de datos altamente optimizados y listos para el negocio: un **Esquema Estrella de Kimball** para análisis de BI basados en SQL (evaluando la distancia de separación entre Receptores y Defensores) y una **One Big Table (OBT)** diseñada para Machine Learning distribuido.

## 🏗️ Arquitectura y Flujo de Datos

1. **Generación de Datos:** Un simulador basado en Python genera telemetría espacial sintética y de alta frecuencia (coordenadas, velocidad, aceleración) de jugadores de la NFL.
2. **Streaming en Tiempo Real:** Los datos se publican en un tópico de Confluent Cloud Kafka y se ingieren directamente en Databricks utilizando Spark Structured Streaming nativo.
3. **Procesamiento de Datos (Arquitectura Medallón):**
   * 🥉 **Bronce:** Ingesta de la carga útil JSON en crudo desde Kafka (solo inserciones/append-only). Captura el estado histórico sin modificaciones.
   * 🥈 **Plata:** Limpieza de datos, aplicación de esquemas (schema enforcement), deduplicación y desempaquetado de estructuras y arreglos (struct/array unpacking).
   * 🥇 **Oro (Analítica):** Modelado Dimensional (`dim_player`, `dim_play`, `dim_game`) y tablas de Hechos (`fact_separation`) utilizando Claves Subrogadas (Hashes MD5).
   * 🥇 **Oro (ML):** Una One Big Table (OBT) con ingeniería de características optimizada para modelado predictivo.

## 🛠️ Tecnologías Utilizadas
* **Motor de Procesamiento de Datos:** Databricks, PySpark, Delta Live Tables (DLT)
* **Streaming de Datos:** Confluent Cloud (Apache Kafka)
* **Modelado de Datos:** Metodología Kimball, OBT (One Big Table)
* **Analítica y Machine Learning:** Spark SQL, PySpark MLlib (Random Forest)
* **Lenguajes:** Python, SQL

## 📂 Estructura del Repositorio

```text
nfl-telemetry-dlt-pipeline/
├── src/
│   ├── 00_data_simulator/
│   │   └── nfl_telemetry_generator.ipynb
│   ├── 02_confluent_to_databricks_streaming/
│   │   └── dlt_confluent_kafka_to_databricks_streaming.py
│   └── 03_dimensions_table_players_plays_game/
│       ├── dlt_dim_tables.py
│       └── dlt_silver_to_gold_fact_separation.py
├── analytics/
│   └── wr_cb_matchup_separation.sql
├── experiments/
│   └── poc_obt_random_forest_validation.ipynb
└── README.md
