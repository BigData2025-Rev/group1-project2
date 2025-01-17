from datetime import date
import pandas as pd

NUM_OF_RECORDS = 15000

ORDERS_START_DATE = date(2021, 1, 1)
ORDERS_END_DATE = date(2025, 1, 1)

CITIES_BY_COUNTRY = {
    "US": 
    (
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
		"Philadelphia", "San Antonio", "San Diego", "Dallas", "Jacksonville",
		"Austin", "Fort Worth", "San Jose", "Columbus", "Charlotte",
		"Indianapolis", "San Francisco", "Seattle", "Denver", "Oklahoma City"
    ),
    "JP": 
    (
        "Tokyo", "Yokohama", "Osaka", "Nagoya", "Sapporo",
		"Fukuoka", "Kobe", "Kawasaki", "Kyoto", "Saitama",
		"Hiroshima", "Sendai", "Chiba", "Kitakyushu", "Sakai"
    ),
    "DE": 
    (
        "Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt am Main",
		"Stuttgart", "Düsseldorf", "Leipzig", "Dortmund", "Essen",
		"Bremen", "Dresden", "Hanover", "Nuremberg", "Duisburg"
    ),
    "FR": 
    (
        "Paris", "Marseille", "Lyon", "Toulouse", "Nice"
    ),
    "IN": 
    (
        "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad",
		"Chennai", "Kolkata", "Surat", "Pune", "Jaipur",
		"Lucknow", "Kanpur", "Nagpur", "Indore", "Thane"
    ),
    "UK": 
    (
        "London", "Birmingham", "Manchester", "Liverpool", "Leeds",
		"Sheffield", "Teesside", "Bristol"
    )
}

COUNTRIES = tuple(CITIES_BY_COUNTRY.keys())

PAYMENT_TYPES = ("Card", "Internet Banking", "UPI", "Wallet")

TXN_FAILURE_RATE = 0.05

PRODUCTS_CSV = './products.csv'

PRODUCTS_DATAFRAME = pd.read_csv(PRODUCTS_CSV)
PRODUCTS_DATAFRAME = PRODUCTS_DATAFRAME[['product_id', 'product_name', 'product_category', 'price']]
PRODUCTS_DATAFRAME['product_name'] = PRODUCTS_DATAFRAME['product_name'].str.replace(r'[^A-Za-z0-9\s]', '', regex=True)

