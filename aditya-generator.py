from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, TimestampNTZType
from faker import Faker
import pyspark.sql
from pyspark.sql import Row
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from enum import Enum
from constants import PAYMENT_TYPES, CITIES_BY_COUNTRY, COUNTRIES, PRODUCTS_DATAFRAME
from datetime import timedelta
from datetime import datetime
import random


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
    region = {"US": 'en_US', "JP": "ja_JP","DE":"de_DE", "FR": "fr_FR",  "IN": "en_IN", "UK": "en_GB"}
    PRODUCTS = PRODUCTS_DATAFRAME.groupby('product_category').sample(n=40, replace=True)
    PRODUCTS = PRODUCTS.to_dict(orient='records')
    ECOMMERCE_WEBSITE = ["amazon", "ebay", "aliexpress", "walmart", "rakuten", "zalando", "etsy"] 
    def __init__(self):
        self.spark = SparkSession.builder.appName("ecom-orders")\
                    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000/user/spark")\
                    .getOrCreate()
        self.gen = self.incrementing_generator(0)
        self.NUM_ROWS = 15000
        header = self.spark.sparkContext.parallelize([tuple(column.value for column in Column)])
        self.rdd = self.spark.sparkContext.parallelize(list(self.generate_row(15000, self.PRODUCTS, PAYMENT_TYPES, COUNTRIES, self.ECOMMERCE_WEBSITE)))
        self.rdd_combined = header.union(self.rdd)
        output = '/user/spark/output'
        self.rdd_combined = self.rdd_combined.map(lambda x: ",".join(map(str, x)))
        self.rdd_coa = self.rdd_combined.coalesce(1)
        self.rdd_coa.saveAsTextFile(output)


    def incrementing_generator(self, start=0):
        while True:
            yield start
            start += 1

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

    def generate_row(self, num_of_rows, products, payment_types,\
                           countries, ecommerce_website_names):
        try:
            for _ in range(num_of_rows):
                country_ = random.choice(countries)
                faker = Faker(self.region[country_])
                start_date = datetime(2021, 1, 1)
                end_date = datetime(2024, 12,13)
                delta = end_date-start_date
                random_seconds = random.randint(0, int(delta.total_seconds()))
                datetime_ = start_date + timedelta(seconds=random_seconds)
                order_id = next(self.gen)
                customer_id = faker.uuid4()
                customer_name = faker.name()
                product = random.choice(products)
                pay_type = random.choice(list(payment_types))
                city_ = random.choice(CITIES_BY_COUNTRY[country_])
                date_time = datetime_
                ecommerce_website_name_ = random.choice(ecommerce_website_names)
                product_id = product['product_id']
                product_name = product['product_name']
                product_category = product['product_category']
                payment_type_ = pay_type
                qty = random.randint(0, 100)
                price = product['price']
                datetime_ = date_time
                country = country_
                city = city_
                ecommerce_website_name = ecommerce_website_name_
                payment_txn_id = faker.uuid4()
                payment_txn_success = random.choices(["Y", "N"], weights=[0.8, 0.2], k=1)[0]
                failure_reason = None
                if payment_txn_success == "N":
                    failure_reason = random.choice(["invalid card details", "card expired"])
                yield (order_id, customer_id, customer_name, product_id,\
                        product_name, product_category,\
                        payment_type_,qty, price, datetime_,\
                        country, city, ecommerce_website_name,\
                        payment_txn_id, payment_txn_success,\
                        failure_reason)
        except Exception as e:
            print(f"Exception {e}")


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
    schema = StructType([ StructField("order_id", IntegerType(), nullable=True),
                          StructField("customer_id", IntegerType(), nullable=True),
                          StructField("customer_name", StringType(), nullable=True),
                          StructField("product_id", IntegerType(), nullable=True),
                          StructField("product_name", StringType(), nullable=True),
                          StructField("product_category", StringType(), nullable=True),
                          StructField("payment_type", StringType(), nullable=True),
                          StructField("qty", IntegerType(), nullable=True),
                          StructField("price", FloatType(), nullable=True),
                          StructField("datetime", TimestampNTZType(), nullable=True),
                          StructField("country", StringType(), nullable=True),
                          StructField("city", StringType(), nullable=True),
                          StructField("ecommerce_website_name", StringType(), nullable=True),
                          StructField("payment_txn_id", IntegerType(), nullable=True),
                          StructField("payment_txn_success", StringType(), nullable=True),
                          StructField("failure_reason", StringType(), nullable=True)])
    generator = Generator()
    generator.spark.stop()
    #generator.csv_to_df()
