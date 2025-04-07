# Databricks notebook source
tiers = ["bronze","silver","gold"]
adls_paths = {tier: f"abfss://{tier}@qwqweeqwewf323fedfsefg.dfs.core.windows.net/" for tier in tiers}

bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)

# COMMAND ----------

import requests
import json
from datetime import date, timedelta

# COMMAND ----------

start_date = date.today() - timedelta(1) #yesterday date
end_date = date.today()  #today date

# COMMAND ----------

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

try:
    response = requests.get(url)

    response.raise_for_status()
    data = response.json().get("features", [])

    if not data:
        print("No data found")
    else:
        file_path = f"{bronze_adls}/{start_date}_earthquake_data.json"
        
        json_data = json.dumps(data, indent=4)
        dbutils.fs.put(file_path, json_data, overwrite=True)
        print(f"data saved successfully to {file_path}")
except requests.exceptions.HTTPError as e:
    print(f"Error: {e}")
        


    


# COMMAND ----------

output_data = {
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "bronze_adls": bronze_adls,
    "silver_adls": silver_adls,
    "gold_adls": gold_adls
}

dbutils.jobs.taskValues.set(key="bronze_output", value=output_data)