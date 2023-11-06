# Databricks notebook source
pip install ucimlrepo

# COMMAND ----------

from ucimlrepo import fetch_ucirepo 
  
# fetch dataset 
iris = fetch_ucirepo(id=53) 
  
# data (as pandas dataframes) 
X = iris.data.features 
y = iris.data.targets 
  
# metadata 
print(iris.metadata) 

# variable information 
display(iris.variables) 


# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd 
from pyspark.sql.types import StructType, StructField, StringType

def dict_to_spark_df(pd_df):
    # Create the scheme based on the first row
    data_dict = pd_df.iloc[0].to_dict()
    
    # Define a schema for the Spark DataFrame
    schema = StructType([
        StructField(field, StringType(), True) for field in data_dict.keys()
   ])
    # Create the Spark DataFrame
    return spark.createDataFrame(pd_df, schema=schema)

# Convert the dictionary to a Spark DataFrame
spark_df = dict_to_spark_df(iris.variables)
display(spark_df)
