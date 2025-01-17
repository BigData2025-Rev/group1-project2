import pyspark.sql
from pyspark.sql import Row
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import datetime
import calendar

from constants import CITIES_BY_COUNTRY, COUNTRIES


COUNTRIES_WEIGHTS = [0.45, 0.10, 0.08, 0.04, 0.25, 0.08]
CITY_WEIGHTS_BY_COUNTRY = { "US":[0.268, 0.126, 0.086, 0.074, 0.056, 0.051, 0.046, 0.046, 0.043, 0.032, 0.033, 0.030, 0.032, 0.029, 0.029, 0.028, 0.026, 0.024, 0.023, 0.022],
                            "JP":[0.405, 0.110, 0.079, 0.068, 0.057, 0.047, 0.045, 0.045, 0.042, 0.039,0.035, 0.031, 0.030, 0.027, 0.024],
                            "DE":[0.258, 0.131, 0.107, 0.074, 0.055, 0.044, 0.043, 0.041, 0.042, 0.041, 0.039, 0.039, 0.037, 0.036, 0.034],
                            "FR":[0.493, 0.198, 0.118, 0.114, 0.077],
                            "IN":[0.164, 0.159, 0.104, 0.085, 0.071, 0.057, 0.036, 0.049, 0.059, 0.040,0.032, 0.024, 0.023, 0.024, 0.020],
                            "UK":[0.645, 0.078, 0.037, 0.034, 0.054, 0.040, 0.046, 0.031]
}

def hightraffic(row):
    years = {2021: 5, 2022: 15, 2023: 30, 2024: 50}
    try:
        date = datetime.datetime.fromisoformat(row.datetime)
        new_year = random.choices(list(years.keys()), list(years.values()), k=1)[0]
        new_month = random.choice([i for i in range(1, 13)])
        days_in_month = calendar.monthrange(new_year, new_month)[1]
        new_day = random.choice([i for i in range(1, days_in_month+1)])
        new_date = date.replace(year=new_year, month=new_month, day=new_day)
    except Exception as e:
        print(f"{e}")

    new_row = Row(order_id=row.order_id, customer_id=row.customer_id,\
                  customer_name=row.customer_name, product_id=row.product_id,\
                  product_name=row.product_name, product_category=row.product_category,\
                  payment_type=row.payment_type, qty=row.qty, price=row.price,\
                  datetime=new_date, country=row.country, city=row.city,\
                  ecommerce_website_name=row.ecommerce_website_name,\
                  payment_txn_id=row.payment_txn_id, payment_txn_success=row.payment_txn_success,\
                  failure_reason=row.failure_reason)
    return new_row

def create_location_sales_trend(row):
    COUNTRY = random.choices(list(COUNTRIES.keys()), COUNTRIES_WEIGHTS, k=1)[0]
    CITY = random.choices(CITIES_BY_COUNTRY[COUNTRY], CITY_WEIGHTS_BY_COUNTRY[COUNTRY], k=1)[0]
    new_row = Row(order_id=row.order_id, customer_id=row.customer_id,\
                  customer_name=row.customer_name, product_id=row.product_id,\
                  product_name=row.product_name, product_category=row.product_category,\
                  payment_type=row.payment_type, qty=row.qty, price=row.price, datetime=row.datetime,\
                  country=COUNTRY, city=CITY, ecommerce_website_name=row.ecommerce_website_name,\
                  payment_txn_id=row.payment_txn_id, payment_txn_success=row.payment_txn_success,\
                  failure_reason=row.failure_reason)
    return new_row

def location_with_high_sales():
    spark = SparkSession.builder.appName("ecom").getOrCreate()
    df = spark.read.option("header", "true").csv("/user/spark/output.csv")
    rdd = df.rdd
    transformed_rdd = rdd.map(create_location_sales_trend)
    transformed_rdd = transformed_rdd.map(hightraffic)
    output = "/user/spark/output"
    out_df = spark.createDataFrame(transformed_rdd)
    out_df.write.option("header", "true").csv(output)
    spark.stop()

location_with_high_sales()
