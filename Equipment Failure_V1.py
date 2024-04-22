# Databricks notebook source
# MAGIC %md
# MAGIC ## ** Equipment Failure Analysis **
# MAGIC Pyspark Notebook to supply information within:
# MAGIC * Total equipment failures that happened?
# MAGIC * Which equipment name had most failures?
# MAGIC * Average amount of failures across equipment group, ordered by the number of failures in ascending order?
# MAGIC * Rank the sensors which present the most number of errors by equipment name in an equipment group.
# MAGIC
# MAGIC #### notes:
# MAGIC | information | data | notes |
# MAGIC |-------------|:-----------:|------------:|
# MAGIC | Dataset used:| 'equipment_sensors.csv', 'equipment.parquet' and 'equpment_failure_sensors.txt' | None |
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Environment Setup
# MAGIC <a id="environment_setup"></a>

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
logger.info("Sessão Spark iniciada.")

# Formatação do nome do arquivo com timestamp
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_dir = "dbfs/FileStore/shared_uploads/mbarbugli@gmail.com"  # Diretório onde os logs serão armazenados
log_filename = f"{log_dir}/total_falhas_{timestamp}.txt"

logger = logging.getLogger(f"{timestamp} [INFO] Equipment Failure Analysis - Inacializando...")
logger.info(f"{timestamp} [INFO] Sessão Spark iniciada.")

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

# logger.info(f"{timestamp} [INFO] Criando tabelas permanentes no metastore...")
# # Criar tabelas permanentes no metastore
# df_sensors.write.saveAsTable("sensors")
# df_equipment.write.saveAsTable("equipment")
# df_logs.write.format("csv").option("header", "false").saveAsTable("logs")
# logger.info(f"{timestamp} [INFO] Tabelas criadas com sucesso!")

log_file_path = f"path_{timestamp}.txt"  # TODO! Atualizar caminho.
# Escrevendo no arquivo de log
try:
    with open(log_file_path, 'w') as file:
        logger.info(f"{timestamp} [INFO] Dados dos logs salvos em {log_file_path}") 
except Exception as e:
    logger.error(f"{timestamp} [ERROR] Erro ao escrever no arquivo: {e}. LOG FILE WAS NOT CREATED! Please check!")

# Criando Alias
df_logs_alias = df_logs.alias("logs")
df_sensors_alias = df_sensors.alias("sensors")
df_equipment_alias = df_equipment.alias("equipment")

logger.info(f"{timestamp} [INFO] Environment Setup finalizado com sucesso!")

# display - TODO:
display(df_sensors)
display(df_equipment)
display(df_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ErrorLogAnalysis
# MAGIC <a id2="ErrorLogAnalysis"></a>

# COMMAND ----------

# DBTITLE 1,ErrorLogAnalysis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("ErrorLogAnalysis").getOrCreate()

logger.info(f"{timestamp} [INFO] Equipment Failure Analysis - Inciando o 'ErrorLogAnalysis'")
logger.info(f"{timestamp} [INFO] Criando tabelas: 'failures_with_equipment' e 'equipment_failure_counts'...") 

# Join
failures_with_equipment = df_logs_alias \
    .join(df_sensors_alias, df_logs_alias["sensor_id"] == df_sensors_alias["sensor_id"]) \
    .join(df_equipment_alias, df_sensors_alias["equipment_id"] == df_equipment_alias["equipment_id"])

logger.info(f"{timestamp} [INFO] Ação realizada com sucesso!") 
logger.info(f"{timestamp} [INFO] Inciando Transformação de dados: ErrorLogAnalysis ") 

# 'count' na coluna 'name' from dataframe 'equipment'
equipment_failure_counts = failures_with_equipment \
    .groupBy(df_equipment_alias["name"]) \
    .count() \
    .orderBy(col("count").desc())

# Criando dataframe equipment_failure_counts
equipment_failure_counts = equipment_failure_counts \
    .withColumnRenamed("name", "equipment_name") \
    .withColumnRenamed("count", "equipment_failure_count")
most_failures_equipment = equipment_failure_counts.first()
logger.info(f"{timestamp} [WARNING] Equipamento com maior numero de falhas: {most_failures_equipment} ") 

# Salvando a tabela no databricks.
logger.info(f"{timestamp} [INFO] Salvando tabelas: {equipment_failure_counts}...") 
equipment_failure_counts.write.option("overwriteSchema", "true").format("delta").mode("overwrite").saveAsTable("most_failures_equipment")
logger.info(f"{timestamp} [INFO] Ação realizada com sucesso!") 
logger.info(f"{timestamp} [INFO] ErrorLogAnalysis - Finalizado com sucesso!") 
display(equipment_failure_counts)




# COMMAND ----------

# MAGIC %md
# MAGIC ### Equipment with Most Failures
# MAGIC <a id4="equipment_most_failures"></a>

# COMMAND ----------

# DBTITLE 1,Equipment with Most Failures
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("EquipmentWithMostFailures").getOrCreate()


logger.info(f"{timestamp} [INFO] Inciando Transformação de dados: Equipment with Most Failures ") 
equipment_failures = equipment_failure_counts.groupBy("equipment_name").count()
most_failures = equipment_failures.orderBy(col("count").desc()).limit(1)

# Extrair o nome do equipamento com o maior número de falhas (em lista)
most_failures_collect = most_failures.collect() 
if most_failures_collect:
    most_failures_row = most_failures_collect[0] 
    equipment_with_most_failures = most_failures_row["equipment_name"]
    logger.info(f"{timestamp} [INFO] Equipamento com maior número de falha: {equipment_with_most_failures}")
else:
    logger.info(f"{timestamp} [INFO] Nenhum dado encontrado para equipamentos com falhas.")

# Verifica se 'most_failures_collect' é uma lista e transforma em dataframe
if isinstance(most_failures_collect, list):
    most_failures_collect = spark.createDataFrame(most_failures_collect)

# Salvando a tabela no databricks.
logger.info(f"{timestamp} [INFO] Criando tabelas: 'most_failures_collect'...") 
most_failures_collect.write.option("overwriteSchema", "true").format("delta").mode("overwrite").saveAsTable("most_failures_collect")
logger.info(f"{timestamp} [INFO] Ação realizada com sucesso!")
display(most_failures)
logger.info(f"{timestamp} [INFO] Equipment with Most Failures - Finalizado com sucesso!") 



# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Failures by Equipment Group
# MAGIC <a id5="average_group"></a>

# COMMAND ----------

# DBTITLE 1,Average Failures by Equipment Group
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger.info(f"{timestamp} [INFO] Inciando Transformação de dados: Average Failures by Equipment Group") 
logger.info(f"{timestamp} [INFO] Criando DataFrame...")

# Criação de dataframes
failures_with_equipment = df_logs_alias.join(
    df_sensors_alias,
    df_logs_alias["sensor_id"] == df_sensors_alias["sensor_id"],
    "inner"
).join(
    df_equipment_alias,
    df_sensors_alias["equipment_id"] == df_equipment_alias["equipment_id"],
    "inner"
)
logger.info(f"{timestamp} [INFO] DataFrame criado com sucesso!") 

# Calculo da media de falhas por Grupo de Equipamento 
logger.info(f"{timestamp} [INFO] Inciando o calculo da media de falhas por Grupo de Equipamento") 
equipment_failure_counts = failures_with_equipment.groupBy(
    df_equipment_alias["equipment_id"].alias("equipment_id"),
    "group_name"
).agg(
    F.count("status").alias("equipment_failure_count")
)

# Calculo total de falhas entre todos os grupos
windowSpec = Window.partitionBy()  # Sem partition
total_failures = F.sum("equipment_failure_count").over(windowSpec)

# Calculo da porcengatem do total de falhas por grupo e criando coluna
equipment_failure_counts = equipment_failure_counts.withColumn(
    "failure_percent",
    F.col("equipment_failure_count") / total_failures * 100
)

# Calculo média por 'group_name'
avg_failures_per_group = equipment_failure_counts.groupBy("group_name").agg(
    F.avg("equipment_failure_count").alias("avg_failures"),
    F.avg("failure_percent").alias("avg_failure_percent")
)

# Ordenando por "'avg_failures' descending" para encontrar grupo com maior numero de falhas
avg_failures_per_group = avg_failures_per_group.orderBy(F.desc("avg_failures"))

# Salvando a tabela no databricks
logger.info(f"{timestamp} [INFO] Criando tabelas: 'avg_failures_per_group'...") 
avg_failures_per_group.write.option("overwriteSchema", "true").format("delta").mode("overwrite").saveAsTable("most_failureavg_failures_per_groups_collect")
logger.info(f"{timestamp} [INFO] Ação realizada com sucesso!")
display(avg_failures_per_group)

logger.info(f"{timestamp} [INFO] Average Failures by Equipment Group - Finalizado com sucesso!") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV
# MAGIC <a id6="csv"></a>

# COMMAND ----------

# DBTITLE 1,CSV
# Importar a sessão Spark
from pyspark.sql import SparkSession

logger.info(f"{timestamp} [INFO] Salvando Resultado em CSV: Inicializando...") 
logger.info(f"{timestamp} [INFO] Criando DataFrame...")
df1 = spark.table("most_failures_equipment")
df2 = spark.table("most_failures_collect")
df3 = spark.table("most_failureavg_failures_per_groups_collect")
logger.info(f"{timestamp} [INFO] Ação realizada com sucesso!")

# DBFS (Databricks File System) onde o arquivo CSV será salvo
output_path1 = f"dbfs:/FileStore/most_failures_equipment_{timestamp}.csv"
output_path2 = f"dbfs:/FileStore/most_failures_collect{timestamp}.csv"
output_path3 = f"dbfs:/FileStore/avg_failures_per_group_{timestamp}.csv"
logger.info(f"{timestamp} [INFO] Salvado CSV: {output_path1}, {output_path2}, {output_path3}")

# Salvar o DataFrame como CSV
df1.write.csv(path=output_path1, mode="overwrite", header=True)
df2.write.csv(path=output_path2, mode="overwrite", header=True)
df3.write.csv(path=output_path3, mode="overwrite", header=True)
logger.info(f"{timestamp} [INFO] Ação realizada com sucesso!")

logger.info(f"{timestamp} [INFO] CSV Saving - Finalizado com sucesso!") 

# Finalizando programa
logger.info(f"{timestamp} [INFO] Finalizando o Equipment Failure - Tech Case") 
logger.info(f"{timestamp} [INFO] Finalizando logs") 

