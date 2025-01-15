from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampNTZType, BooleanType
from faker import Faker
import random

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
    
Fields that Faker can generate values for
    customer_name
    country
    city
    ecommerce_website_name

"""

# Function to generate a row of data
def generate_data(order_id):
    return (
        order_id,
        random.randint(18, 80),
        fake.name(),
        random.randint(18, 80),
        fake.word(),
        fake.name(),
        fake.name(),
        random.randint(18, 80),
        round(random.uniform(1.00, 500.00), 2),
        fake.date_time(),
        fake.country(),
        fake.city(),
        fake.domain_name(),
        random.randint(18, 80),
        bool(random.randint(0, 1)),
        fake.word()
    )

# Define the schema
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

# Generate synthetic data for 15,000 rows
#data = [generate_data() for _ in range(15000)]
data = [generate_data(_) for _ in range(1, 101)]

# Create a DataFrame from the generated data
df = spark.createDataFrame(data, schema)

df.select("order_id", "customer_id",  "customer_name", "product_id", "product_name", "price", "ecommerce_website_name").show()

