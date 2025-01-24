from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("ryan-data-analysis")
         .getOrCreate())

if __name__ == "__main__":
    df = spark.read.csv("data_clean.csv", header=True)

    df.show(5, vertical=True)

    print(f"Total # of Records: {df.count()}")
    print(f"Total # of distinct Records: {df.distinct().count()}")
    print()

    """Order IDS"""
    order_ids = df.select(col("order_id"))
    print("5 order_ids from the top")
    top5 = order_ids.take(5)
    for row in top5:
        print(row.__getitem__('order_id'))
    print(f"# of distinct order_ids: {order_ids.distinct().count()}")
    print()

    """TXN IDS"""
    txn_ids = df.select(col("payment_txn_id"))
    print("5 txn_ids from the top")
    top5 = txn_ids.take(5)
    for row in top5:
        print(row.__getitem__('payment_txn_id'))
    print(f"# of distinct txn_ids: {txn_ids.distinct().count()}")
    print()

    """CUSTOMER IDS"""
    cust_ids = df.select(col("customer_id"))
    print("5 customer_ids from the top")
    top5 = cust_ids.take(5)
    for row in top5:
        print(row.__getitem__('customer_id'))
    print(f"# of distinct customer_ids: {cust_ids.distinct().count()}")
    print()

    """RETURN CUSTOMERS"""
    customers = df.select(df.customer_id, df.customer_name, df.country, df.city).groupBy(["customer_id", "customer_name", "country", "city"]).count()
    customers.show()
    print(f"# of distinct customers (id, name, city, country): {customers.distinct().count()}")
    print(f"# of distinct customer_names: {df.select(col('customer_name')).distinct().count()}")
    print()

    """INSPECT ROWS WITH DUPLICATE ORDER_ID"""
    dup = df.dropDuplicates(['order_id'])
    dup = df.subtract(dup)
    dup = dup.alias("dup")
    dup = dup.join(df.alias('df'), col('df.order_id') == col('dup.order_id'), 'inner')
    print(f"# of dup rows: {dup.count()}")
    print(f"# of distinct dup rows: {dup.distinct().count()}")
    dup.select(col('df.*')).show(10)

    """CHECK FOR NULL VALUES"""
    no_null = df.drop(col('failure_reason')).dropna()
    print(f"# of rows without null values: {no_null.count()}")

    """PRODUCT_CATEGORIES"""
    product_categories = df.select(col("product_category"))
    print("5 product_categories from the top")
    top5 = product_categories.take(5)
    for row in top5:
        print(row.__getitem__('product_category'))
    print(f"# of distinct product_categories: {product_categories.distinct().count()}")
    print()
