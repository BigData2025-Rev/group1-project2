from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, TimestampNTZType
from faker import Faker
import pyspark.sql
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from enum import Enum


class Column(Enum):
    order_id = "order_id"
    customer_id = "customer_id"
    customer_name = "customer_name"
    product_id = "product_id"
    product_name = "product_name"
    product_category = "product_category"
    payment_type = "payment_type"
    qty = "qty"
    price = "price"
    datetime = "datetime"
    country = "country"
    city = "city"
    ecommerce_website_name = "ecommerce_website_name"
    payment_txn_id = "payment_txn_id"
    payment_txn_success = "payment_txn_success"
    failure_reason = "failure_reason"


class Generator:
    faker = Faker()
    spark = SparkSession.builder.appName("ecom-orders")\
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000/user/spark")\
                .getOrCreate()

    def csv_to_df(self):
        directory = "hdfs://localhost:9000/user/spark/newarchive/"
        spark_df = self.spark.read.option("header", "true").option("ignoreLeadingWhiteSpace", "true").csv(directory+"*.csv")
        table_name = "products_table"
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark_df = spark_df.select("name", "main_category", "actual_price")
        spark_df = spark_df.withColumn("product_id", monotonically_increasing_id())
        spark_df = spark_df.withColumnRenamed("name", "product_name")\
                           .withColumnRenamed("main_category", "product_category")\
                           .withColumnRenamed("actual_price", "price")
        spark_df.createOrReplaceTempView(table_name)
        df = self.spark.sql("SELECT product_id, product_name, product_category, price from products_table")
        df.coalesce(1).write.csv("/user/spark/output/", header=True)
        self.spark.stop()

    def generate_row(self, order_id):
        customer_id = None
        product_id = None
        product_name = None
        product_category = None
        payment_type = None
        qty = None
        price = None
        datetime = None
        country = None
        city = None
        ecommerce_website_name = None
        payment_txn_id = None
        payment_txn_success = None
        failure_reason = None
        return (order_id, customer_id, product_id,
                product_name, product_category, payment_type, 
                qty, price, datetime, 
                country, city, ecommerce_website_name, 
                payment_txn_id, payment_txn_success, failure_reason)


    def create_location_sales_trend(self, country=[], country_weights=[],\
                                          cities=[], cities_weights=[],\
                                          products=[], products_weights=[],\
                                          fraction_of_rows_transformed=0.0):
        self.df = self.df.sample(withReplacement=False, fraction=fraction_of_rows_transformed)  # Sample 10% of the rows if percentage is 0.1
        random_product = random.choices(products, products_weights, k=1)
        self.df = self.df\
                .withColumn(Column.country, random.choices(countries, country_weights, k=1))\
                .withColumn(Column.city, random.choices(cities, cities_weights, k=1))\
                .withColumn(Column.product_id, random_product['product_id'])\
                .withColumn(Column.product_name, random_product['product_name'])\
                .withColumn(Column.product_category, random_product['product_category'])\
                .withColumn(Column.price, random_product['price'])


if __name__=='__main__':
    schema = StructType([ StructField("order_id", IntegerType(), nullable=False),
                          StructField("customer_id", IntegerType(), nullable=True),
                          StructField("customer_name", StringType(), nullable=True),
                          StructField("product_id", IntegerType(), nullable=True),
                          StructField("product_name", StringType(), nullable=True),
                          StructField("product_category", StringType(), nullable=True),
                          StructField("payment_type", StringType(), nullable=True),
                          StructField("qty", IntegerType(), nullable=True),
                          StructField("price", FloatType(), nullable=False), 
                          StructField("datetime", TimestampNTZType(), nullable=False),
                          StructField("country", StringType(), nullable=False),
                          StructField("city", StringType(), nullable=False),
                          StructField("ecommerce_website_name", StringType(), nullable=False),
                          StructField("payment_txn_id", IntegerType(), nullable=False),
                          StructField("payment_txn_success", StringType(), nullable=False),
                          StructField("failure_reason", StringType(), nullable=False)])
    generator = Generator()
    generator.csv_to_df()
