from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampNTZType
from faker import Faker
import random
import constants
from datetime import date

fake = Faker()

spark = SparkSession.builder.appName("group1-data-generator").getOrCreate()

"""Notes
About prices
    Product prices for a given product should be in a given, semi-realistic range.
    The identical products should have identical prices.
    
About qty
    No one should be buying 20 heaters, but some people will probably buy 20 energy drinks. 
    
About customers
    Realistically, there will be repeat customers. That means they will have the same id and name across multiple orders.
    For simplicity's sake, these sort of customers would also be tied to a city/country.
    However, these factors may not be necessary to account for unless there is an interesting trend to develop from it.

About payment_txn_id
    What is the difference between this and order_id?

About payment_txn_success
    Most orders should have successful payments. 95%+

About payment_type and failure_reason
    Failure reason should either match the payment type, or be vague. Ideally, we match the payment type.
    ex: Failure reason should not be "Invalid CVV" for customers who paid without a card.
    
Fields that Faker can generate values for
    customer_name
    ecommerce_website_name

"""


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
    
def generate_country():
    countries = constants.COUNTRIES
    return countries[random.randint(0, len(countries)-1)]

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
    country = generate_country()
    city = generate_city(country)

    return (
        order_id,
        random.randint(1, constants.NUM_OF_RECORDS),
        fake.name(),
        random.randint(1, 100),
        fake.word(),
        fake.name(),
        payment_type,
        random.randint(1, 20),
        round(random.uniform(1.00, 500.00), 2),
        fake.date_time_between(constants.ORDERS_START_DATE, constants.ORDERS_END_DATE),
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

# Generate synthetic data for 15,000 rows
#data = [generate_data() for _ in range(1, constants.NUM_OF_RECORDS+1)]
data = [generate_row(_) for _ in range(1, 101)]

# Create a DataFrame from the generated data
df = spark.createDataFrame(data, schema)

#df.select("order_id", "customer_id",  "customer_name", "product_id", "product_name", "price", "ecommerce_website_name").show()
#df.select("order_id", "datetime", "payment_type", "payment_txn_id", "payment_txn_success", "failure_reason").show()
df.select("order_id", "datetime", "country", "city", "payment_type", "payment_txn_success").show()

