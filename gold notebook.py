# Databricks notebook source
bronze_output = dbutils.jobs.taskValues.get(taskKey= "bronze", key="bronze_output")
silver_output = dbutils.jobs.taskValues.get(taskKey= "silver", key="silver_output")

start_date = bronze_output.get("start_date")
end_date = bronze_output.get("end_date")
bronze_adls = bronze_output.get("bronze_adls")
silver_adls = bronze_output.get("silver_adls")
gold_adls = bronze_output.get("gold_adls")

print(f"start_date: {start_date}, bronze_adls: {gold_adls}")

# COMMAND ----------

from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType
import reverse_geocoder as rg
from datetime import date, timedelta

# COMMAND ----------

df = spark.read.parquet(silver_output).filter(col("time") > start_date)

# COMMAND ----------

df = df.limit(10)

# COMMAND ----------

def get_country_code(lat,lon):

    try:
        coordinates = (float(lat), float(lon))
        result = rg.search(coordinates)[0]['cc']  #cc is built-in to the output returned by the reverse_geocoder library — it stands for Country Code.
         
        print(f"proccesing coordinates: {coordinates} => {result}")
        return result
    except Exception as e:
        print(f"Error proccesing coordinates: {lat},{lon} => str{e}")
        return None

# COMMAND ----------

# register udf into spark dataframe
get_country_code_udf = udf(get_country_code, StringType()) 


# COMMAND ----------

get_country_code(25.276987, 55.296249)

# COMMAND ----------

df_with_location = df.\
        withColumn("country_code", get_country_code_udf(col("latitude"), col("longitude")))

# COMMAND ----------


df_with_sig_class = \
    df_with_location.\
        withColumn(
            "sig_class",
            when(col("sig") < 100, "low")
            .when((col("sig") >= 100) & (col("sig") < 500), "moderate")
            .otherwise("high")
        )


# COMMAND ----------

df_with_sig_class.head()

# COMMAND ----------

gold_output= f"{gold_adls}earthquake_events_gold/"

# COMMAND ----------

df_with_sig_class.write.mode("append").parquet(gold_output)