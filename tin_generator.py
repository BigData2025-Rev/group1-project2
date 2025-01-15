import random
import string
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampNTZType, BooleanType
from faker import Faker

ROWS = 1000
SEED = 1234
OUTPUT_PATH = "data"
CHARACTERS = string.ascii_letters + string.digits

Faker.seed(SEED)
random.seed(SEED)

spark = SparkSession.builder.appName("group1-data-generator").getOrCreate()
fake = Faker()

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
    customer_id = generate_id(10)

    product_id, product_name, product_category, product_price = generate_product()
    payment_type = generate_payment_type()
    qty = random.randint(18, 80)
    price = round(product_price * qty, 2)
    ecommerce_website_name = generate_ecommerce_website_name()

    payment_txn_id = generate_id(12)
    payment_txn_success = bool(random.randint(0, 1))
    if payment_txn_success:
        failure_reason = ""
    else:
        failure_reason = generate_failure_reason(payment_type)

    return (
        order_id,
        customer_id,
        fake.name(),
        product_id,
        product_name,
        product_category,
        payment_type,
        qty,
        price,
        fake.date_time(),
        fake.country(),
        fake.city(),
        ecommerce_website_name,
        payment_txn_id,
        payment_txn_success,
        failure_reason
    )

def generate_id(length):
    return ''.join(random.choices(CHARACTERS, k=length))

def generate_product():
    # List of products from a csv files
    return 1, "", "", 1.25

def generate_payment_type():
    options = ["Debit Card", "Credit Card", "Paypal", "Venmo", "Gift Card", "Checking Account"]
    weights = [1, 2, 3, 1, 5, 1]
    return random.choices(options, weights=weights)[0]

def generate_ecommerce_website_name():
    options = ["Amazon", "AliExpress", "eBay", "Temu", "Walmart", "Etsy"]
    weights = [8, 2, 3, 1, 4, 1]
    return random.choices(options, weights=weights)[0]

def generate_failure_reason(payment_type):
    return f"Reason based on {payment_type}"

if __name__ == "__main__":
    # Define the schema
    schema = StructType([ StructField("order_id", IntegerType(), nullable=False),
                        StructField("customer_id", StringType(), nullable=True),
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
                        StructField("payment_txn_id", StringType(), nullable=False),
                        StructField("payment_txn_success", BooleanType(), nullable=False),
                        StructField("failure_reason", StringType(), nullable=False)])

    # Generate data
    data = [generate_data(i) for i in range(1, ROWS + 1)]

    # Create a DataFrame from the generated data
    df = spark.createDataFrame(data, schema)

    df.show(n=5, truncate=False)

    # Write to a csv file
    df.coalesce(1).write.csv(OUTPUT_PATH, header=True, mode="append")