import random
import string
from pyspark.sql import SparkSession
from faker import Faker
from constants import PRODUCTS_DF, PRODUCTS_CATEGORY, PAYMENT_TYPES, FAILURE_REASON, COUNTRIES, CITIES_BY_COUNTRY, ECOMMERCE_WEBSITE


ROWS = 15000
SEED = 1234
OUTPUT_PATH = "data"
CHARACTERS = string.ascii_letters + string.digits

Faker.seed(SEED)
random.seed(SEED)

spark = SparkSession.builder.appName("group1-data-generator").getOrCreate()
fake = Faker()


def generate_data(order_id):
    customer_id = generate_id(10)

    product_id, product_name, product_category, product_price = generate_product()
    payment_type = generate_payment_type()
    qty = random.randint(18, 80)
    price = round(product_price * qty, 2)
    ecommerce_website_name = generate_ecommerce_website_name()

    payment_txn_id = generate_id(12)
    chance = random.randint(1, 10)
    if chance <= 8:
        payment_txn_success = True
        failure_reason = ""
    else:
        payment_txn_success = False
        failure_reason = generate_failure_reason(payment_type)

    country, city = generate_place()
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
        country,
        city,
        ecommerce_website_name,
        payment_txn_id,
        payment_txn_success,
        failure_reason
    )

def generate_id(length):
    return ''.join(random.choices(CHARACTERS, k=length))

def generate_product():
    categories = list(PRODUCTS_CATEGORY.keys())
    weights = list(PRODUCTS_CATEGORY.values())

    chosen_category = random.choices(categories, weights=weights)[0]

    row = PRODUCTS_DF[PRODUCTS_DF["category"] == chosen_category].sample(n=1, random_state=SEED + random.randint(1, 1000))

    p_id, p_name, p_category, p_price = row.iloc[0]
    return int(p_id), p_name, p_category, float(p_price)

def generate_payment_type():
    options = list(PAYMENT_TYPES.keys())
    weights = list(PAYMENT_TYPES.values())
    return random.choices(options, weights=weights)[0]

def generate_ecommerce_website_name():
    options = list(ECOMMERCE_WEBSITE.keys())
    weights = list(ECOMMERCE_WEBSITE.values())
    return random.choices(options, weights=weights)[0]

def generate_failure_reason(payment_type):
    payment_type_reasons = FAILURE_REASON[payment_type]
    options = list(payment_type_reasons.keys())
    weights = list(payment_type_reasons.values())
    return random.choices(options, weights=weights)[0]

def generate_place():
    options = list(COUNTRIES.keys())
    weights = list(COUNTRIES.values())
    country = random.choices(options, weights=weights)[0]

    city = random.choice(CITIES_BY_COUNTRY[country])

    return country, city

if __name__ == "__main__":
    columns = ["order_id", "customer_id", "customer_name", "product_id", "product_name", "product_category", "payment_type", "qty", "price", "datetime", "country", "city", "ecommerce_website_name", "payment_txn_id", "payment_txn_success", "failure_reason"]

    rdd = spark.sparkContext.parallelize([generate_data(i) for i in range(1, ROWS + 1)])

    df = rdd.toDF(columns)

    df.show(n=5, truncate=False)

    # Write to a csv file
    df.coalesce(1).write.csv(OUTPUT_PATH, header=True, mode="append")