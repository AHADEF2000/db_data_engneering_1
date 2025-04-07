# Databricks notebook source
bronze_output = dbutils.jobs.taskValues.get(taskKey= "bronze", key="bronze_output")

start_date = bronze_output.get("start_date")
end_date = bronze_output.get("end_date")
bronze_adls = bronze_output.get("bronze_adls")
silver_adls = bronze_output.get("silver_adls")

print(f"start_date: {start_date}, bronze_adls: {bronze_adls}")


# COMMAND ----------

tiers = ["bronze","silver","gold"]
adls_paths = {tier: f"abfss://{tier}@qwqweeqwewf323fedfsefg.dfs.core.windows.net/" for tier in tiers}

bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

silver_output= f"{silver_adls}earthquake_events_silver/"

dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)

# COMMAND ----------

from datetime import date, timedelta

start_date = date.today() - timedelta(1)
end_date = date.today()

# COMMAND ----------

from pyspark.sql.functions import col, isnull, when
from pyspark.sql.types import TimestampType

# COMMAND ----------

df = spark.read.option("multiLine", True).json(f"{bronze_adls}{start_date}_earthquake_data.json") # read the json file as a spark dataframe

# COMMAND ----------

df.head()

# COMMAND ----------

df = (
    df
    .select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('evaluation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
    )
)

# COMMAND ----------

df.head()

# COMMAND ----------

#validating the data, check there is null value in the data
df = (
    df
    .withColumn('longitude', when(isnull(col('longitude')), 0).otherwise(col('longitude')))
    .withColumn('latitude', when(isnull(col('latitude')), 0).otherwise(col('latitude')))
    .withColumn('time', when(isnull(col('time')), 0).otherwise(col('time')))
)

# COMMAND ----------

df = (
    df
    .withColumn('time', (col('time')/1000).cast(TimestampType()))
    .withColumn('updated', (col('updated')/1000).cast(TimestampType()))
)
    

# COMMAND ----------

silver_output_path = f"{silver_adls}earthquake_events_silver/" # creating the silver output in a folder inside the silver path called earthquake_events_silver

# COMMAND ----------

df.write.mode("append").parquet(silver_output_path)

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "silver_output", value= silver_output_path)