# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Reference: https://www.databricks.com/notebooks/gallery/SanFranciscoFireCallsAnalysis.html

# COMMAND ----------

# DBTITLE 1,if you want just a part of this file - Run this
#if you want just a partial part of this file.

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.sfgov.org", None)

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("nuek-vuh3", limit=2000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

# COMMAND ----------

# DBTITLE 1,if you want the entire file! Run it
from pyspark.sql.types import *
from pyspark.sql.functions import *

# URL of the CSV file
csv_url = "https://data.sfgov.org/api/views/nuek-vuh3/rows.csv"
sf_fire_file = "/st2/sf_fire_calls_database.csv"

#download and store the file, it's a HUGE file!
dbutils.fs.cp(csv_url, sf_fire_file)

#Define our schema as the file has more than 6 million records (6.391.161). Inferring the schema is expensive for large files. 
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),      
                     StructField('WatchDate', StringType(), True),
                     StructField('ReceivedDtTm', StringType(), True),
                     StructField('EntryDtTm', StringType(), True),
                     StructField('DispatchDtTm', StringType(), True),
                     StructField('ResponseDtTm', StringType(), True),
                     StructField('OnSceneDtTm', StringType(), True),
                     StructField('TransportDtTm', StringType(), True),
                     StructField('HospitalDtTm', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('Zipcode', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('CaseLocation', StringType(), True),
                     StructField('AnalysisNeighborhoods', StringType(), True)])
                     
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
display(fire_df)

# COMMAND ----------

# DBTITLE 1,Take a look on the file
# MAGIC %fs head "/st2/sf_fire_calls_database.csv"

# COMMAND ----------

# DBTITLE 1,To check the number of calls in this database
fire_df.count()

# COMMAND ----------

#Filter out "Medical Incident" call types
#Note that filter() and where() methods on the DataFrame are similar. Check relevant documentation for their respective argument types
few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType") 
              .where(col("CallType") != "Medical Incident"))
 
display(few_fire_df)

# COMMAND ----------

# DBTITLE 1,Q-1) How many distinct types of calls were made to the Fire Department?
print(fire_df.select("CallType")
      .where(col("CallType").isNotNull())
      .distinct()
      .count())

# COMMAND ----------

# DBTITLE 1,Q-2) What are distinct types of calls were made to the Fire Department?
display(fire_df.select("CallType")
        .where(col("CallType").isNotNull())
        .distinct())

# COMMAND ----------

# DBTITLE 1,Q-3) Find out all response or delayed times greater than 5 mins?
#Calculate the response time.
new_fire_df = fire_df.withColumn(
    "ResponseDelayedinSecs",
    (unix_timestamp("OnSceneDtTm", "MM/dd/yyyy hh:mm:ss a") - unix_timestamp("ReceivedDtTm", "MM/dd/yyyy hh:mm:ss a"))
)

display(new_fire_df
        .select("ResponseDelayedinSecs")
        .where(col("ResponseDelayedinSecs") > (5 * 60)))

# COMMAND ----------

#Let's do some ETL:
#Transform the string dates to Spark Timestamp data type so we can make some time-based queries later

fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate") 
              .withColumn("OnWatchDate",   to_timestamp(col("WatchDate"), "MM/dd/yyyy")).drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")) 

display(fire_ts_df)

# COMMAND ----------

# DBTITLE 1,Q-4) What were the most common call types?
display(fire_ts_df
 .select("CallType").where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .limit(10))

# COMMAND ----------

display(fire_ts_df
 .select("CallType").where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .limit(20)
 .toPandas()  # Convert the result to a Pandas DataFrame
 .plot(kind='bar', x='CallType', y='count'))

# COMMAND ----------

display(fire_ts_df
        .select("Neighborhood", "Zipcode")
        .where((col("Zipcode") == 94102) | (col("Zipcode") == 94103))
        .distinct())

# COMMAND ----------

# DBTITLE 1,Q-4b) What San Francisco neighborhoods are in the zip codes 94102 and 94103
display(fire_ts_df
        .select("Neighborhood", "Zipcode")
        .where((col("Zipcode") == 94102) | (col("Zipcode") == 94103))
        .distinct()
        .toPandas()
        .plot(kind='bar', x='Neighborhood', y='Zipcode'))

# COMMAND ----------

# DBTITLE 1,Q-5) What was the sum of all calls, average, min and max of the response times for calls?
#filter the response time ahead of time! they predict it!
display(fire_ts_df
        .select("NumAlarms","ResponseDelayedinSecs").where(col("ResponseDelayedinSecs") > 0)
        .select(sum("NumAlarms"), 
        avg("ResponseDelayedinSecs").alias("AvgResponseDelayedinSecs"), 
        min("ResponseDelayedinSecs").alias("MinResponseDelayedinSecs") , 
        max("ResponseDelayedinSecs").alias("MaxResponseDelayedinSecs"))
        )

# COMMAND ----------

# DBTITLE 1,Q-6a) How many distinct years of data is in the CSV file?
display(fire_ts_df
    .select(year('IncidentDate'))
    .distinct()
    .orderBy(year('IncidentDate').desc()))

# COMMAND ----------

# DBTITLE 1,Q-6b) What week of the year in 2023 had the most fire calls?
display(fire_ts_df
        .filter(year('IncidentDate') == 2023)
        .groupBy(weekofyear('IncidentDate'))
        .count()
        .orderBy('count', ascending=False))

# COMMAND ----------

# DBTITLE 1,Q-7) What top 20 neighborhoods in San Francisco had the worst response time in 2018?
display(fire_ts_df
        .select("Neighborhood", "ResponseDelayedinSecs")
        .where(year("IncidentDate") == 2023)
        .limit(20)
        .toPandas()
        .plot(kind='bar', x='Neighborhood', y='ResponseDelayedinSecs'))
