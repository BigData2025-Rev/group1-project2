import pandas as pd

PRODUCTS_DF = pd.read_csv('./tin_products.csv')
PRODUCTS_CATEGORY = {
    'accessories': 10,
    'appliances': 30,
    'toys & baby products': 30,
    'car & motorbike': 15,
    'industrial supplies': 5,
    'sports & fitness': 10
}

PAYMENT_TYPES = {
    "Debit Card": 10,
    "Credit Card": 40,
    "Gift Card": 20,
    "Bank Transfer": 10,
    "Digital Wallet": 20
}
FAILURE_REASON = {
    "Debit Card": {
        "Incorrect card details (number, expiry, CVV)": 10,
        "Insufficient funds": 20
    },
    "Credit Card": {
        "Incorrect card details (number, expiry, CVV)": 10,
        "credit limit exceeded": 20
    },
    "Gift Card": {
        "Insufficient balance": 10,
        "Expired card": 20
    },
    "Bank Transfer": {
        "Network or bank server downtime": 20,
        "Wrong bank details": 10
    },
    "Digital Wallet": {
        "Wallet not funded": 10,
        "Authentication errors": 20
    }
}

COUNTRIES = {
    "US": 50,
    "JP": 10,
    "DE": 5,
    "FR": 5,
    "IN": 5,
    "UK": 25
}

CITIES_BY_COUNTRY = {
    "US": 
    (
        "New York", "Los Angeles", "Chicago", "Houston", "Peoenix",
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
		"Bremen", "Dresdent", "Hanover", "Nuremburg", "Duisburg"
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

ECOMMERCE_WEBSITE = {
    "Amazon": 50,
    "AliExpress": 10,
    "eBay": 5,
    "Temu": 5,
    "Walmart": 5,
    "Etsy": 25
}
