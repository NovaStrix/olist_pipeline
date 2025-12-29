#Import pyspark for data wrangling
from pyspark.sql.types import *
from pyspark.sql import SparkSession

import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine

#Import Pandas
import pandas as pd

#Make dirnames
datasets = [
    d for d in os.listdir("/opt/airflow/data/output")
    if os.path.isdir(os.path.join("/opt/airflow/data/output/", d))
]
#Get database connection info from environment variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

#state vars
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"



def load_to_dwh():
    #Create Spark session
    spark = SparkSession.builder.appName("Data Loading").getOrCreate()

    #Create connection to Supabase Postgres
    engine = create_engine(DATABASE_URL)

    dfs = {}

    for dataset in datasets:
        df = spark.read.parquet(f"/opt/airflow/data/output/{dataset}")
        dfs[dataset] = df.toPandas()
        print("____________________________________________________")
        print(f"Loaded {dataset} with {len(dfs[dataset])} records for loading.")
        print(f"Schema for {dataset}:")
        print(df.schema)
        #Load each dataframe to Supabase
        dfs[dataset].to_sql(dataset, engine, if_exists='replace', index=False)
        print(f"Loaded {dataset} to Supabase Postgres.")

    spark.stop()
    print("____________________________________________________")
    print("Data loading to DWH completed successfully.")








if __name__ == '__main__':
    load_to_dwh()