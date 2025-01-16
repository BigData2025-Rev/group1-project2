from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, when, month, year
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampNTZType
from faker import Faker
import random
import constants
from datetime import date

fake = Faker()

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("group1-data-generator")
         .getOrCreate())

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
product_categories = ("Heating Appliances", "Baby Products")

def add_month(d:date) -> date:
    year, month, day = d.year, d.month, d.day
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    return date(year, month, day)

def generate_product_category():
    return product_categories[random.randint(0, len(product_categories)-1)]

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
    product_category = generate_product_category()

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

def show_records_each_month(df:DataFrame):
    """Just a helper function to see records, not meant for final product"""
    start_date = date(2021, 1, 1)
    end_date = date(2021, 2, 1)
    while end_date < date(2025, 1, 1):
        # noinspection PyTypeChecker
        dff = df.filter((col("product_category") == "Baby Products") & (col("datetime") > start_date) & (col("datetime") < end_date))
        if not dff.isEmpty():
            print(start_date)
            print(end_date)
            dff.select("order_id", "product_category", "qty", "datetime").show()
        start_date = add_month(start_date)
        end_date = add_month(end_date)

def change_record_dates_by_month(df:DataFrame, weight_by_month):
    # TODO: Account for quantity when moving dates to particular months.
    out_df = df.alias("out_df")
    start_date = constants.ORDERS_START_DATE
    end_date = date(start_date.year, start_date.month+1, start_date.day)
    while end_date < constants.ORDERS_END_DATE:
        for mon in weight_by_month:
            # noinspection PyTypeChecker
            dff = df.filter((col("product_category") == "Baby Products") & (col("datetime") > end_date) & (year(col("datetime")) == start_date.year))
            if not dff.isEmpty():
                # TODO: Use another method other than sample() to get rows to alter
                dff = dff.sample(withReplacement=False, fraction= 1.00 * weight_by_month[mon])
                print(f"Sample: {dff.count()}")
                rows = dff.collect()
                order_ids = [row["order_id"] for row in rows]
                dates = [row["datetime"] for row in rows]
                for i in range(len(dates)):
                    dates[i] = fake.date_time_between(start_date, end_date)
                rows.clear()
                for d in dates:
                    rows.append(Row(date_df_datetime=d))
                data = []
                for i in range(len(order_ids)):
                    data.append((order_ids[i], dates[i]))
                if data:
                    date_df = spark.createDataFrame(data, ['date_df_order_id', 'date_df_datetime'])
                    print(f"Sample: {dff.count()}")
                    join_df = dff.join(date_df, (date_df["date_df_order_id"] == dff["order_id"]), "left")
                    join_df = join_df.select(col('*'))
                    join_df = join_df.withColumn("datetime", col("date_df_datetime"))
                    join_df = join_df.drop(*date_df.columns)
                    out_df.union(join_df)
            start_date = add_month(start_date)
            end_date = add_month(end_date)
    else:
        return out_df

if __name__ == "__main__":
    # Generate synthetic data for 15,000 rows
    # data = [generate_data() for _ in range(1, constants.NUM_OF_RECORDS+1)]
    data = [generate_row(_) for _ in range(1, 1001)]

    # Create a DataFrame from the generated data
    df = spark.createDataFrame(data, schema)

    # Transform dataframe to infuse a trend.
    weight_by_month = {
        1:  0.05,
        2:  0.04,
        3:  0.05,
        4:  0.06,
        5:  0.07,
        6:  0.10,
        7:  0.12,
        8:  0.15,
        9:  0.13,
        10: 0.11,
        11: 0.07,
        12: 0.05
    }

    df = change_records_each_month(df, weight_by_month)
    show_records_each_month(df)
    print(df.count())
