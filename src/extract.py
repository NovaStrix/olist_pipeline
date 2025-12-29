#Import pyspark for data wrangling
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, when, regexp_replace

#Import kaggle for data extraction
import kagglehub

#Import utils
import shutil
import os

#List of filenames in the dataset
filenames = [
"olist_customers_dataset.csv",
"olist_geolocation_dataset.csv",
"olist_order_items_dataset.csv",
"olist_order_payments_dataset.csv",
"olist_order_reviews_dataset.csv",
"olist_orders_dataset.csv",
"olist_products_dataset.csv",
"olist_sellers_dataset.csv",
"product_category_name_translation.csv"
]

def extract_data():
    #Clean data/raw if exists

    if os.path.exists("/opt/airflow/data/raw"):
        shutil.rmtree("/opt/airflow/data/raw")
        os.makedirs("/opt/airflow/data/raw", exist_ok=True)

    #Get data from Kaggle and put to data/raw
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    print("Path to dataset files:", path)

    #Create Spark session
    spark = SparkSession.builder.appName("Data Extraction").getOrCreate()
    
    #Read CSV files into DataFrames

    dirnames = []
    dfs = {}
    null_counts = 0
    for filename in filenames:
        df = spark.read.csv(f"{path}/{filename}", header=True)
        filename = filename.replace("_dataset.csv", "").replace(".csv", "")
        dfs[filename] = df
        print("____________________________________________________")
        print(f"Loaded {filename} with {df.count()} records.")
        dirnames.append(filename)
        #Check for null values in each column
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                print("____________________________________________________")
                print(f"Column {column} ({dfs[filename].schema[column].dataType}) in {filename} has {null_count} null values.")
                null_counts += null_count
        
    if null_counts == 0:
        print("____________________________________________________")
        print("No null values found in any dataset.")
    else:
        print("____________________________________________________")
        print(f"Total null values found across datasets: {null_counts}")
    
    #Save cleaned data back to data/raw
    for filename in dirnames:
        dfs[filename].write.mode("overwrite").csv(f"/opt/airflow/data/raw/{filename}", header=True)
        print(f"Saved cleaned {filename} to data/raw/{filename}")

    #Return list of directory names
    print(dirnames)

    spark.stop()

if __name__ == "__main__":
    extract_data()# src/extract.py