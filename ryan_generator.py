from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, when, month, year
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampNTZType
from faker import Faker
import random
import constants
import pandas as pd
from datetime import date, time, datetime

fake = Faker()

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("group1-data-generator")
         .getOrCreate())

df = pd.read_csv("output.csv")
product_categories = df["product_category"].unique()

category_weights = constants.CATEGORY_WEIGHTS
country_weights = constants.COUNTRY_WEIGHTS

def generate_product_category():
    total_weights = [category_weights[category]['base_weight'] for category in category_weights]
    return random.choices([category for category in category_weights], total_weights, k=1)[0]

def generate_country(category):
    return random.choices(constants.COUNTRIES, [country_weights[country]['base_weight'] * category_weights[category]['weight_by_country'][country] for country in country_weights], k=1)[0]

def generate_order_month(category):
    return random.choices([mon for mon in category_weights[category]['weight_by_month']], [weight for weight in category_weights[category]["weight_by_month"].values()], k=1)[0]

def generate_order_year(category):
    return random.choices([year for year in category_weights[category]['weight_by_year']], [weight for weight in category_weights[category]["weight_by_year"].values()], k=1)[0]

def generate_order_time(country):
    """
    morning:    [7am,12pm)
    afternoon:  [12pm,5pm)
    evening:     [5pm,10pm)
    """
    day_period = random.choices([day_period for day_period in country_weights[country]['weight_by_time']],
                                [weight for weight in country_weights[country]["weight_by_time"].values()], k=1)[0]
    start_hour = 0; end_hour = 23
    match day_period:
        case 'morning':
            start_hour = 7; end_hour = 12
        case 'afternoon':
            start_hour = 12; end_hour = 17
        case 'evening':
            start_hour = 17; end_hour = 22
    return fake.date_time_between(datetime(2000, 1, 1, start_hour),
                           datetime(2000, 1, 1, end_hour)).time()

def add_month(d:date) -> date:
    year, month, day = d.year, d.month, d.day
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    return date(year, month, day)

def construct_order_datetime(year, month, time):
    start_date = date(year, month, 1)
    end_date = add_month(start_date)
    d = fake.date_time_between(start_date, end_date)
    return d.replace(hour=time.hour, minute=time.minute, second=time.second, microsecond=time.microsecond)

def generate_payment_txn_success():
    if random.uniform(0.00, 100.00) <= constants.TXN_FAILURE_RATE * 100:
        return "N"
    else:
        return "Y"

def generate_payment_type():
    payment_types = constants.PAYMENT_TYPES
    return payment_types[random.randint(0, len(payment_types)-1)]

    
def generate_failure_reason(payment_type):
    if payment_type == "Card":
        return "Invalid CVV"
    else:
        return "Balance too low"

def generate_city(country):
    cities_by_country = constants.CITIES_BY_COUNTRY
    cities = cities_by_country[country]
    return cities[random.randint(0, len(cities)-1)]

def generate_row(order_id):
    """Generates a row of data."""
    payment_type = generate_payment_type()
    payment_txn_success = generate_payment_txn_success()
    failure_reason = ""
    if payment_txn_success == "N":
        failure_reason = generate_failure_reason(payment_type)
    product_category = generate_product_category()
    country = generate_country(product_category)
    city = generate_city(country)
    order_year = generate_order_year(product_category)
    order_month = generate_order_month(product_category)
    order_time = generate_order_time(country)
    order_datetime = construct_order_datetime(order_year, order_month, order_time)

    return (
        order_id,
        random.randint(1, constants.NUM_OF_RECORDS),
        fake.name(),
        random.randint(1, 100),
        fake.word(),
        product_category,
        payment_type,
        random.randint(1, 20),
        round(random.uniform(1.00, 500.00), 2),
        order_datetime,
        country,
        city,
        fake.domain_name(),
        order_id + 1000,
        payment_txn_success,
        failure_reason
    )

# Define the schema
schema = StructType([ StructField("order_id", IntegerType(), nullable=False),
                      StructField("customer_id", IntegerType(), nullable=True),
                      StructField("customer_name", StringType(), nullable=True),
                      StructField("product_id", IntegerType(), nullable=True),
                      StructField("product_name", StringType(), nullable=True),
                      StructField("product_category", StringType(), nullable=True),
                      StructField("payment_type", StringType(), nullable=False),
                      StructField("qty", IntegerType(), nullable=True),
                      StructField("price", FloatType(), nullable=False), 
                      StructField("datetime", TimestampNTZType(), nullable=False),
                      StructField("country", StringType(), nullable=False),
                      StructField("city", StringType(), nullable=False),
                      StructField("ecommerce_website_name", StringType(), nullable=False),
                      StructField("payment_txn_id", IntegerType(), nullable=False),
                      StructField("payment_txn_success", StringType(), nullable=False),
                      StructField("failure_reason", StringType(), nullable=True)])

def show_records_each_month(df:DataFrame):
    """Just a helper function to see records, not meant for final product"""
    start_date = date(2021, 1, 1)
    end_date = date(2021, 2, 1)
    while end_date < date(2025, 1, 1):
        # noinspection PyTypeChecker
        dff = df.filter((col("product_category") == "toys & baby products") & (col("datetime") > start_date) & (col("datetime") < end_date))
        if not dff.isEmpty():
            print(start_date)
            print(end_date)
            dff.select("order_id", "product_category", "qty", "datetime").show()
        start_date = add_month(start_date)
        end_date = add_month(end_date)

if __name__ == "__main__":
    # Generate synthetic data for 15,000 rows
    # data = [generate_data() for _ in range(1, constants.NUM_OF_RECORDS+1)]
    data = [generate_row(_) for _ in range(1, 10001)]

    # Create a DataFrame from the generated data
    df = spark.createDataFrame(data, schema)

    # Transform dataframe to infuse a trend.
    #df = df.select("order_id","product_category","country","datetime").filter(col("product_category") == "toys & baby products")
    # df = df.select("order_id", "product_category", "country", "datetime")
    # df.show()
    # print(df.count())
    # #df = df.filter(month(col("datetime")) == 8)
    print(df.filter((year(col("datetime")) == 2021) & (col("product_category") == 'toys & baby products')).count())
    print(df.filter((year(col("datetime")) == 2022) & (col("product_category") == 'toys & baby products')).count())
    print(df.filter((year(col("datetime")) == 2023) & (col("product_category") == 'toys & baby products')).count())
    print(df.filter((year(col("datetime")) == 2024) & (col("product_category") == 'toys & baby products')).count())
    # #df = df.filter(col("country") == 'US')
    df2 = df.select("order_id","product_category","country","datetime").filter((col("product_category") == 'toys & baby products'))
    df2.show()
    # print("total category")
    # print(df2.count())
    # df33 = df.filter((col("country") == 'DE'))
    # df3 = df.filter((col("country") == 'DE') & (col("product_category") == 'accessories'))
    # print("in JP (total, then indust)")
    # print(df33.count())
    # print(df3.count())
    # df4 = df.filter((col("country") == 'UK'))
    # df = df.filter((col("country") == 'UK') & (col("product_category") == 'accessories'))
    # print("in US (should be ~5x JP)")
    # print(df4.count())
    # print(df.count())
    """
    show_records_each_month(df)
    print(df.count())
    """

