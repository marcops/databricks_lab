# Databricks notebook source
# DBTITLE 1,Load the library necessary for read the XLSX
pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Load the class responsible to download, extract and transform the XLSX in a Spark Dataframe
import requests
import zipfile
import io
import os
import pandas as pd
import shutil
import tempfile

#This is the first version, and only accept the zipped XLSX content.
#Stay tune for new vesions
class XLSXLoaderV1:
    def __init__(self, zip_url):
        self.zip_url = zip_url
        self.temp_dir = None

    def get_spark_df(self):
        zip_data = self.download()
        df = self.extract(zip_data)
        self.cleanup()
        return df
    
    @staticmethod
    def xlsx_to_df(self, xlsx_file):
        # Read data from the XLSX file into a Pandas DataFrame
        pandas_df = pd.read_excel(xlsx_file)
        # Convert the Pandas DataFrame to a PySpark DataFrame
        return spark.createDataFrame(pandas_df)
    
    @staticmethod
    def extract(self, zip_data):
        self.temp_dir = tempfile.mkdtemp()
        os.makedirs(self.temp_dir, exist_ok=True)

        # Extract the zip file to the temporary directory
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            zip_ref.extractall(self.temp_dir)

        # Find the XLSX file within the temporary directory
        xlsx_files = [f for f in os.listdir(self.temp_dir) if f.lower().endswith('.xlsx')]
        if not xlsx_files:
            raise Exception("No XLSX file found in the extracted content")

        xlsx_file = os.path.join(self.temp_dir, xlsx_files[0])
        
        return self._xlsx_to_df(xlsx_file)
    
    @staticmethod
    def download(self):
        # Send an HTTP GET request to download the zip file
        response = requests.get(self.zip_url)

        # Check if the request was successful (status code 200)
        if response.status_code != 200:
            raise Exception(f"Failed to download the zip file. Status code: {response.status_code}")

        # Create a BytesIO object to work with the zip file content
        return io.BytesIO(response.content)
    
    @staticmethod
    def cleanup(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

# COMMAND ----------

# DBTITLE 1,Load and Add Business Rules
from pyspark.sql.functions import col

zip_url = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"

spark_df_with_sales_amount = (XLSXLoaderV1(zip_url)
    .get_spark_df()
    .withColumn("SalesAmount", col("Quantity") * col("UnitPrice")))

display(spark_df_with_sales_amount)
