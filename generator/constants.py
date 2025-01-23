import pandas as pd

PRODUCTS_DF = pd.read_csv('generator/products.csv')
PRODUCTS_CATEGORY = {
    'accessories': 12,
    'appliances': 24,
    'toys & baby products': 20,
    'car & motorbike': 20,
    'industrial supplies': 8,
    'sports & fitness': 16 
}

PAYMENT_TYPES = {
    "Debit Card": 10,
    "Credit Card": 35,
    "Gift Card": 25,
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
        "credit limit exceeded": 50
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

MONTH_WEIGHTS_BY_CATEGORY = {
    'accessories': [0.05, 0.04, 0.05, 0.06, 0.07, 0.10, 0.12, 0.15, 0.13, 0.11, 0.07, 0.05],
    'appliances': [0.09, 0.06, 0.05, 0.04, 0.02, 0.02, 0.05, 0.10, 0.15, 0.20, 0.12, 0.10],
    'toys & baby products': [0.05, 0.04, 0.05, 0.06, 0.07, 0.10, 0.12, 0.15, 0.13, 0.11, 0.07, 0.05],
    'car & motorbike': [0.05, 0.04, 0.05, 0.06, 0.07, 0.10, 0.12, 0.15, 0.13, 0.11, 0.07, 0.05],
    'industrial supplies': [0.05, 0.04, 0.05, 0.06, 0.07, 0.10, 0.12, 0.15, 0.13, 0.11, 0.07, 0.05],
    'sports & fitness': [0.05, 0.04, 0.05, 0.06, 0.07, 0.10, 0.12, 0.15, 0.13, 0.11, 0.07, 0.05] 
}

TIME_WEIGHTS_BY_COUNTRY = {
    "US": [5, 5, 2, 2, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 1, 1, 1, 1, 1, 1, 1, 1],
    "JP": [1, 1, 1, 1, 5, 5, 5, 10, 5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    "DE": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 6, 7, 6, 5, 1, 1, 1, 1, 1, 1, 1, 1],
    "FR": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 2, 2, 5, 10, 4, 1, 1, 1, 1, 1, 1],
    "IN": [1, 1, 4, 5, 6, 5, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    "UK": [1, 1, 4, 1, 1, 1, 1, 1, 3, 4, 5, 6, 6, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
}

YEARS = {
    "2021": 5, 
    "2022": 15,
    "2023": 30,
    "2024": 50
}

COUNTRIES = ["US", "JP", "DE", "FR", "IN", "UK"]
COUNTRY_WEIGHTS = {
    "US": 0.40,
    "JP": 0.15,
    "DE": 0.10,
    "FR": 0.05,
    "IN": 0.20,
    "UK": 0.10,
}
CATEGORY_WEIGHTS = {
    "appliances": {
        "weight_by_country":
            {
                "US": .15,
                "JP": .15,
                "DE": .10,
                "FR": .10,
                "IN": .20,
                "UK": .30
            }
    },
    "toys & baby products": {
        "weight_by_country":
            {
                "US": .20,
                "JP": .05,
                "DE": .10,
                "FR": .25,
                "IN": .25,
                "UK": .15
            }
    },
    "accessories": {
        "weight_by_country":
            {
                "US": .15,
                "JP": .30,
                "DE": .10,
                "FR": .20,
                "IN": .15,
                "UK": .10
            }
    },
    "industrial supplies": {
        "weight_by_country":
            {
                "US": .30,
                "JP": .05,
                "DE": .10,
                "FR": .10,
                "IN": .30,
                "UK": .15
            }
    },
    "sports & fitness": {
        "weight_by_country":
            {
                "US": .10,
                "JP": .20,
                "DE": .30,
                "FR": .15,
                "IN": .15,
                "UK": .10
            }
    },
    "car & motorbike": {
        "weight_by_country":
            {
                "US": .30,
                "JP": .10,
                "DE": .10,
                "FR": .25,
                "IN": .05,
                "UK": .20
            }
    }
}