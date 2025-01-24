import pandas as pd
import random
import string
from datetime import datetime

# Read the CSV file
df = pd.read_csv('part-00000-fa7b175a-b967-457c-9dce-5444ec64d559-c000.csv')
df = df.map(str)

def rouge_string(val):
    is_blank = random.choices([True, False], [0.3, 0.7], k=1)[0]
    if is_blank:
        return ''
    return 'Unknown'

def replace_year_with_random(timestamp):
    dt = datetime.fromisoformat(timestamp)
    random_year = random.randint(1960, 1990)
    new_timestamp = dt.replace(year=random_year)
    return new_timestamp.isoformat()

def check_rouge():
    rows_with_unknown = df.map(lambda x: x == "Unknown" or x == '' or x.startswith("$")).any(axis=1)

    print(f"Number of rows with at least one 'Unknown': {rows_with_unknown.sum()}")

check_rouge()
random_rows = df.sample(n=250)
df.loc[random_rows.index, 'product_id'] = df.loc[random_rows.index, 'product_id'].apply(rouge_string)

check_rouge()
random_rows = df.sample(n=100)
df.loc[random_rows.index, 'country'] = df.loc[random_rows.index, 'country'].apply(rouge_string)

check_rouge()
random_rows = df.sample(n=50)
df.loc[random_rows.index, 'customer_name'] = df.loc[random_rows.index, 'customer_name'].apply(rouge_string)

check_rouge()
random_rows = df.sample(n=250)
df.loc[random_rows.index, 'price'] = df.loc[random_rows.index, 'price'].apply(lambda x: "$" + x)

check_rouge()
random_rows = df.sample(n=100)
df.loc[random_rows.index, 'datetime'] = df.loc[random_rows.index, 'datetime'].apply(replace_year_with_random)

check_rouge()

df.to_csv('modified_file.csv', index=False)