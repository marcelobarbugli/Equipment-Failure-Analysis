# Databricks notebook source
# MAGIC %md
# MAGIC # Equipment-Failure-Analysis
# MAGIC * Developed by Marcelo Dozzi Barbugli
# MAGIC * Dataset: csv, parquet, txt
# MAGIC * Function and Job: Data Analysis and Data Transformation
# MAGIC * Notes: Notes table
# MAGIC __________________________________________________________________
# MAGIC
# MAGIC
# MAGIC ### Equipment Failure Analysis Description
# MAGIC Pyspark Notebook to supply information within these questions:
# MAGIC * Total equipment failures that happened?
# MAGIC * Which equipment name had most failures?
# MAGIC * Average amount of failures across equipment group, ordered by the number of failures in ascending order?
# MAGIC * Rank the sensors which present the most number of errors by equipment name in an equipment group.
# MAGIC
# MAGIC ### Dataset
# MAGIC * You can find the dataset when Fork or Clone this repository
# MAGIC * Files: “equipment_failure_sensors.txt”; “equipment_sensors.csv”; “equipment.json”
# MAGIC
# MAGIC ### Function and Job
# MAGIC * TODO documentation
# MAGIC
# MAGIC ### notes:
# MAGIC | information | data | notes |
# MAGIC |-------------|:-----------:|------------:|
# MAGIC | Dataset | 'equipment_sensors.csv', 'equipment.parquet' and 'equpment_failure_sensors.txt' | None |
# MAGIC | Improvements | v2 expects improvement for better performance | 1. OOP development; 2. improve validations of each step and cache; 3. bugfix |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Environment Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
import os
from datetime import datetime
import logging


spark = SparkSession.builder \
    .appName("Equipment Failure Analysis") \
    .config("spark.some.config.option", "some-value") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuração do Logger para registrar informações
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Equipment Failure Analysis")
# Formatação do nome do arquivo com timestamp
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_dir = "dbfs/FileStore/shared_uploads/mbarbugli@gmail.com"
log_filename = f"{log_dir}/total_falhas_{timestamp}.txt"

logger = logging.getLogger(f"{timestamp} [INFO] Equipment Failure Analysis - Inicializando...")
logger.info(f"{timestamp} [INFO] Sessão Spark iniciada.")

# COMMAND ----------

# DBTITLE 1,Extract
# Ler os dados
df_sensors = spark.read.csv("dbfs:/FileStore/equipment_sensors.csv", header=True, inferSchema=True)
df_equipment = spark.read.parquet("dbfs:/FileStore/equipment.parquet")
df_logs = spark.read.option("delimiter", "\t").csv("dbfs:/FileStore/equpment_failure_sensors.txt", header=False, inferSchema=True)

logger.info(f"{timestamp} [INFO] Os dados foram carregados com sucesso!")
logger.info(f"{timestamp} [INFO] Atualizando Schema: {df_logs}")

# Schema
df_logs = df_logs.toDF("date", "status", "raw_sensor_data", "temperature", "vibration", "signal")
df_logs = df_logs.withColumn(
    "sensor_id", 
    regexp_extract("raw_sensor_data", r"sensor\[(\d+)\]:", 1)
)
# df_logs = df_logs.join(df_sensors, "sensor_id").join(df_equipment, "equipment_id")
logger.info(f"{timestamp} [UPDATE] O Schema: {df_logs}, foi atualizado com sucesso!")

# COMMAND ----------

# DBTITLE 1,Log Setup
log_file_path = f"dbfs:/FileStore/log_{timestamp}_equipment_failure.txt"  # TODO! Atualizar caminho.
# Escrevendo no arquivo de log
try:
    with open(log_file_path, 'w') as file:
        logger.info(f"{timestamp} [INFO] Dados dos logs salvos em {log_file_path}") 
except Exception as e:
    logger.error(f"{timestamp} [ERROR] {e}. LOG FILE WAS NOT CREATED! Please check!")


# COMMAND ----------

# DBTITLE 1,Alias and DF

# Criando Alias
df_logs_alias = df_logs.alias("logs")
df_sensors_alias = df_sensors.alias("sensors")
df_equipment_alias = df_equipment.alias("equipment")

# Salvando 
df_logs_alias.write.mode('overwrite').parquet("dbfs:/FileStore/workflow/equpment_failure_sensors_loaded.parquet")
df_sensors_alias.write.mode('overwrite').parquet("dbfs:/FileStore/workflow/df_sensors_loaded.parquet")
df_equipment_alias.write.mode('overwrite').parquet("dbfs:/FileStore/workflow/equipment_loaded.parquet")


# # display - TODO:
# display(df_sensors)
# display(df_equipment)
# display(df_logs)
logger.info(f"{timestamp} [INFO] Environment Setup finalizado com sucesso!")

# COMMAND ----------

# DBTITLE 1,ErrorLogAnalysis Run
# MAGIC %run /Shared/ErrorLogAnalysis

# COMMAND ----------

# DBTITLE 1,Equipment with Most Failures
# MAGIC %run /Shared/EquipMostFailures
# MAGIC

# COMMAND ----------

# DBTITLE 1,Average Failures by Equipment Group
# MAGIC %run /Shared/Average_Failures_by_EquipmentGroup
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,CSV
# MAGIC %run /Shared/CsvWriterShape

# COMMAND ----------

# Finalizando programa
logger.info(f"{timestamp} [INFO] Finalizando o Equipment Failure - Tech Case") 
logger.info(f"{timestamp} [INFO] Finalizando logs") 
