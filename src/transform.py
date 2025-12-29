#Import pyspark for data wrangling
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

#Import dirnames from cleaning.py
from cleaning import dirnames

#Import utils
import shutil
import os

dfs = {}

##Make tables functions for transformation
###Fact Tables
os.listdir('/opt/airflow/data/processed')
def fact_orders():
    df = dfs['olist_orders']

    ##Make is_delivered column
    df = df.withColumn('is_delivered', when(col('order_delivered_customer_date').isNotNull(), lit(1)).otherwise(lit(0)))

    return df.select(
        col("(order_id IS NOT NULL)").alias("order_id"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
        col("is_delivered")
    )

def fact_order_items():
    df = dfs['olist_order_items']
    return df.select(
        col("(order_id IS NOT NULL)").alias("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("seller_id"),
        col("shipping_limit_date"),
        col("price"),
        col("freight_value")
    )

def fact_payments():
    df = dfs['olist_order_payments']
    return df.select(
        col("(order_id IS NOT NULL)").alias("order_id"),
        col("payment_sequential"),
        col("payment_type"),
        col("payment_installments"),
        col("payment_value")
    )

def fact_reviews():
    df = dfs['olist_order_reviews']
    df = df.withColumn('has_review',when(col('review_comment_message').isNotNull(), lit(1)).otherwise(lit(0)))
    return df.select(
        col("review_id"),
        col("(order_id IS NOT NULL)").alias("order_id"),
        col("has_review"),
        col("review_score"),
        col("review_comment_title"),
        col("review_comment_message"),
        col("review_creation_date"),
        col("review_answer_timestamp")
    )

###Dimension Tables
def dim_customers():
    df = dfs['olist_customers']
    return df.select(
        col("(customer_id IS NOT NULL)").alias("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state")
    )

def dim_products():
    df = dfs['olist_products']
    return df.select(
        col("(product_id IS NOT NULL)").alias("product_id"),
        col("product_category_name"),
        col("product_name_lenght"),
        col("product_description_lenght"),
        col("product_photos_qty"),
        col("product_weight_g").cast("double"),
        col("product_length_cm").cast("double"),
        col("product_height_cm").cast("double"),
        col("product_width_cm").cast("double")
    )

def dim_sellers():
    df = dfs['olist_sellers']
    return df.select(
        col("(seller_id IS NOT NULL)").alias("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state")
    )

table_names = []
def transform():
    #Check for existing file
    if os.path.exists('data/output'):
        shutil.rmtree("data/output")
        os.makedirs("data/output", exist_ok=True)

    #Create Spark session
    spark = SparkSession.builder.appName("Data_Transformation").getOrCreate()

    #Read cleaned data from data/processed
    for dirname in dirnames:
        df = spark.read.parquet(f"/opt/airflow/data/processed/{dirname}")
        dfs[dirname] = df
        print("____________________________________________________")
        print(f"Loaded {dirname} with {df.count()} records for transformation.")

        #print schema
        dfs[dirname].printSchema()
    
    #Table Functions for transformation
    print('=====================================================')
    print('printing transformed dataframes now...')
    print('=====================================================')
    #Fact Tables
    fact_orders_df = fact_orders()
    fact_order_items_df = fact_order_items()
    fact_payments_df = fact_payments()
    fact_reviews_df = fact_reviews()

    #Dimension Tables
    dim_customers_df = dim_customers()
    dim_products_df = dim_products()
    dim_sellers_df = dim_sellers()

    print('=====================================================')
    print('writing transformed dataframes to data/output now...')
    print('=====================================================')
    
    #Write transformed data to data/output in parquet format
    fact_orders_df.write.mode('overwrite').parquet("/opt/airflow/data/output/fact_orders")
    fact_order_items_df.write.mode("overwrite").parquet("/opt/airflow/data/output/fact_order_items")
    fact_payments_df.write.mode("overwrite").parquet("/opt/airflow/data/output/fact_payments")
    fact_reviews_df.write.mode("overwrite").parquet("/opt/airflow/data/output/fact_reviews")
    dim_customers_df.write.mode("overwrite").parquet("/opt/airflow/data/output/dim_customers")
    dim_products_df.write.mode("overwrite").parquet("/opt/airflow/data/output/dim_products")
    dim_sellers_df.write.mode("overwrite").parquet("/opt/airflow/data/output/dim_sellers")

             
    spark.stop()

if __name__ == '__main__':
    transform()
