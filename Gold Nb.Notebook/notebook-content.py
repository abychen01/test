# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2f73d050-1f8b-4e9d-bd8e-6640ac335a1c",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "8be724d1-75c7-4a15-9b26-dc6747947d8b",
# META       "known_lakehouses": [
# META         {
# META           "id": "2f73d050-1f8b-4e9d-bd8e-6640ac335a1c"
# META         },
# META         {
# META           "id": "a389b12c-e773-475f-91eb-e98c19bc40da"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "21929c7e-d48b-b087-49d5-48d4f0b8dc8a",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

silver_lh = ""
gold_lh = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import reverse_geocoder as rg
import pycountry as pyc

from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType, StructType, StructField
from datetime import date, timedelta


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Gold_LH.gold_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#silver_lh2 = "Silver_LH.Silver_data"
#gold_lh2 = "Gold_LH.Gold_data"

df = spark.read.table(silver_lh)

def get_location_details(lat, long):
    try:
        coordinates = (float(lat), float(long))
        result = rg.search(coordinates)[0]
        return (result.get('name'), result.get('cc'), result.get('admin1'))  
    except Exception as e:
        print(f"Error processing coordinates: {lat}, {long} -> {str(e)}")
        return (None, None, None)

def get_country_name(code):
    try:
        result = pyc.countries.get(alpha_2 = code.upper())
        return result.name if result else None
    except Exception as e:
        return None


# Define UDF return schema
result_schema = StructType([
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("State", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Register UDF
location_udf = udf(get_location_details, result_schema)
country_name_udf = udf(get_country_name, StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Apply single UDF and extract both fields
df = (df
      .withColumn("location_data", location_udf(col("latitude"), col("longitude")))
      .withColumn("City", col("location_data.City"))
      .withColumn("Country_Code", col("location_data.Country"))
        .withColumn("State", col("location_data.State"))
      .drop("location_data"))
df = df.withColumn("Country", country_name_udf(col("Country_Code")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("append").saveAsTable(gold_lh)
#df.write.format("delta").mode("append").saveAsTable(gold_lh2)
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# import reverse_geocoder as rg  # Ensure this package is installed
# 
# def get_location_details(lat, long):
#     try:
#         coordinates = (float(lat), float(long))
#         result = rg.search(coordinates)[0]
#         return (result.get('name'), result.get('cc'), result.get('admin1'))  # (City, Country Code)
#     except Exception as e:
#         print(f"Error processing coordinates: {lat}, {long} -> {str(e)}")
#         return (None, None, None)

# MARKDOWN ********************

# # Returns tuple: (city_name, country_code)
# city, country_code, state = get_location_details(37.7749, -122.4194)
# 
# print(city)         # Output: 'San Francisco'
# print(country_code) # Output: 'US'
# print(state)
