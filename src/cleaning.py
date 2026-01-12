#Import pyspark for data wrangling
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit

#Import dirnames from extract.py
from extract import filenames

#Import utils
import shutil
import os

#Make dirnames
dirnames = [filename.replace("_dataset.csv", "").replace(".csv", "") for filename in filenames]


#Make cleaning function
def clean_customers():
    return dfs['olist_customers'].filter(col("customer_id").isNotNull()).select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state")
    )

def clean_geolocation():
    df = (
        dfs['olist_geolocation']
        .withColumn("geolocation_lat", col("geolocation_lat").cast("double"))
        .withColumn("geolocation_lng", col("geolocation_lng").cast("double"))
    )

    return df.filter(
        col("geolocation_lat").isNotNull() &
        col("geolocation_lng").isNotNull()
    )

def clean_order_items():
    return dfs['olist_order_items'].filter(col("order_id").isNotNull()).select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("seller_id"),
        col("shipping_limit_date"),
        col("price").cast("double"),
        col("freight_value").cast("double")
    )

def clean_order_payments():
    return dfs['olist_order_payments'].filter(col("order_id").isNotNull()).select(
        col("order_id"),
        col("payment_sequential").cast("int"),
        col("payment_type"),
        col("payment_installments").cast("int"),
        col("payment_value").cast("double")
    )


def clean_order_reviews():
    return dfs["olist_order_reviews"].filter(col("order_id").isNotNull()).select(
        col("review_id"),
        col("order_id"),
        col("review_score").cast("int").alias("review_score"),
        col("review_comment_title"),
        col("review_comment_message"),
        to_timestamp(col("review_creation_date")).alias("review_creation_date"),
        to_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp"),
    )

def clean_orders():
    return dfs['olist_orders'].filter(col("order_id").isNotNull()).select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp").cast("timestamp"),
        col("order_approved_at").cast("timestamp"),
        to_timestamp(col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
        to_timestamp(col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
        to_timestamp(col("order_estimated_delivery_date")).alias("order_estimated_delivery_date")
    )

def clean_products():
    return dfs['olist_products'].filter(col("product_id").isNotNull()).select(
        col("product_id"),
        col("product_category_name"),
        col("product_name_lenght").cast("int"),
        col("product_description_lenght").cast("int"),
        col("product_photos_qty").cast("int"),
        col("product_weight_g").cast("double"),
        col("product_length_cm").cast("double"),
        col("product_height_cm").cast("double"),
        col("product_width_cm").cast("double")
    )

def clean_sellers():
    return dfs['olist_sellers'].filter(col("seller_id").isNotNull()).select(
        col("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state")
    )

def last_order_date():
    dirnames.append('last_order_date')
    return dfs['olist_orders'].groupBy("customer_id").agg({"order_purchase_timestamp": "max"}).withColumnRenamed("max(order_purchase_timestamp)", "last_order_date")

dfs = {}

def clean_data():
    #Clean /opt/airflow/data/processed if exists
    if os.path.exists("/opt/airflow/data/processed"):
        shutil.rmtree("/opt/airflow/data/processed")
        os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    
    #Create Spark session
    spark = SparkSession.builder.appName("Data_Cleaning").getOrCreate()
    
    #Read raw data from /opt/airflow/data/raw
    for dirname in dirnames:
        df = spark.read.csv(f"/opt/airflow/data/raw/{dirname}", header=True)
        dfs[dirname] = df
        print("____________________________________________________")
        print(f"Loaded {dirname} with {df.count()} records for cleaning.")

        #print schema
        dfs[dirname].printSchema()
    
    #Notifier
    print('=====================================================')
    print('printing cleaned dataframes now...')
    print('=====================================================')  

    #clean tables specifically
    dfs['olist_customers'] = clean_customers()
    dfs['olist_geolocation'] = clean_geolocation()
    dfs['olist_order_items'] = clean_order_items()
    dfs['olist_order_payments'] = clean_order_payments()
    dfs['olist_order_reviews'] = clean_order_reviews()
    dfs['olist_orders'] = clean_orders()
    dfs['olist_products'] = clean_products()
    dfs['olist_sellers'] = clean_sellers()
    dfs['last_order_date'] = last_order_date()

    for dirname in dirnames:
        print("____________________________________________________")
        print(f"Cleaned {dirname} schema:")
        dfs[dirname].printSchema()
        print(f"Cleaned {dirname} has {dfs[dirname].count()} records.")

    #Save cleaned data back to /opt/airflow/data/processed
    for dirname in dirnames:
        dfs[dirname].write.mode("overwrite").parquet(f"/opt/airflow/data/processed/{dirname}")
        print(f"Saved cleaned {dirname} to /opt/airflow/data/processed/{dirname}")
    
    spark.stop()


if __name__ == "__main__":
    clean_data()