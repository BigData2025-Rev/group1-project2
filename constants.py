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

#TODO: Don't need weight_by_category?
COUNTRY_WEIGHTS = {
    "US":
        {
        "base_weight": 0.40,
        "weight_by_category":
            {
            "appliances": 0.15,
            "toys & baby products": 0.15,
            "accessories": 0.15,
            "industrial supplies": 0.15,
            "sports & fitness": 0.10,
            "car & motorbike": 0.30
            },
        "weight_by_time":
            {
                "morning": 0.30,
                "afternoon": 0.20,
                "evening": 0.50
            }
        },
    "JP":
        {
        "base_weight": 0.15,
        "weight_by_category":
            {
            "appliances": 0.15,
            "toys & baby products": 0.15,
            "accessories": 0.15,
            "industrial supplies": 0.15,
            "sports & fitness": 0.10,
            "car & motorbike": 0.10
            },
        "weight_by_time":
            {
                "morning": 0.50,
                "afternoon": 0.30,
                "evening": 0.20
            }
        },
    "DE":
        {
        "base_weight": 0.10,
        "weight_by_category":
            {
            "appliances": 0.15,
            "toys & baby products": 0.15,
            "accessories": 0.15,
            "industrial supplies": 0.15,
            "sports & fitness": 0.10,
            "car & motorbike": 0.10
            },
        "weight_by_time":
            {
                "morning": 0.15,
                "afternoon": 0.50,
                "evening": 0.35
            }
        },
    "FR":
        {
        "base_weight": 0.05,
        "weight_by_category":
            {
            "appliances": 0.15,
            "toys & baby products": 0.15,
            "accessories": 0.15,
            "industrial supplies": 0.15,
            "sports & fitness": 0.10,
            "car & motorbike": 0.25
            },
        "weight_by_time":
            {
                "morning": 0.20,
                "afternoon": 0.35,
                "evening": 0.55
            }
        },
    "IN":
        {
        "base_weight": 0.20,
        "weight_by_category":
            {
            "appliances": 0.15,
            "toys & baby products": 0.15,
            "accessories": 0.15,
            "industrial supplies": 0.15,
            "sports & fitness": 0.10,
            "car & motorbike": 0.05
            },
        "weight_by_time":
            {
                "morning": 0.25,
                "afternoon": 0.50,
                "evening": 0.25
            }
        },
    "UK":
        {
        "base_weight": 0.10,
        "weight_by_category":
            {
            "appliances": 0.15,
            "toys & baby products": 0.15,
            "accessories": 0.15,
            "industrial supplies": 0.15,
            "sports & fitness": 0.10,
            "car & motorbike": 0.20
            },
        "weight_by_time":
            {
                "morning": 0.30,
                "afternoon": 0.40,
                "evening": 0.30
            }
        }
}

CATEGORY_WEIGHTS = {
    "appliances": {
        "base_weight": 0.15,
        "weight_by_month":
            {
                1: 0.09,
                2: 0.06,
                3: 0.05,
                4: 0.04,
                5: 0.02,
                6: 0.02,
                7: 0.05,
                8: 0.10,
                9: 0.15,
                10: 0.20,
                11: 0.12,
                12: 0.10
            },
        "weight_by_year":
            {
                2021: 0.30,
                2022: 0.25,
                2023: 0.20,
                2024: 0.20
            },
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
        "base_weight": 0.25,
        "weight_by_month":
            {
                1: 0.05,
                2: 0.04,
                3: 0.05,
                4: 0.06,
                5: 0.07,
                6: 0.10,
                7: 0.12,
                8: 0.15,
                9: 0.13,
                10: 0.11,
                11: 0.07,
                12: 0.05
            },
        "weight_by_year":
            {
                2021: 0.10,
                2022: 0.20,
                2023: 0.30,
                2024: 0.40
            },
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
        "base_weight": 0.05,
        "weight_by_month":
            {
                1: 0.05,
                2: 0.04,
                3: 0.05,
                4: 0.06,
                5: 0.07,
                6: 0.10,
                7: 0.12,
                8: 0.15,
                9: 0.13,
                10: 0.11,
                11: 0.07,
                12: 0.05
            },
        "weight_by_year":
            {
                2021: 0.10,
                2022: 0.15,
                2023: 0.35,
                2024: 0.20
            },
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
        "base_weight": 0.15,
        "weight_by_month":
            {
                1: 0.05,
                2: 0.04,
                3: 0.05,
                4: 0.06,
                5: 0.07,
                6: 0.10,
                7: 0.12,
                8: 0.15,
                9: 0.13,
                10: 0.11,
                11: 0.07,
                12: 0.05
            },
        "weight_by_year":
            {
                2021: 0.20,
                2022: 0.40,
                2023: 0.30,
                2024: 0.20
            },
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
        "base_weight": 0.25,
        "weight_by_month":
            {
                1: 0.05,
                2: 0.04,
                3: 0.05,
                4: 0.06,
                5: 0.07,
                6: 0.10,
                7: 0.12,
                8: 0.15,
                9: 0.13,
                10: 0.11,
                11: 0.07,
                12: 0.05
            },
        "weight_by_year":
            {
                2021: 0.10,
                2022: 0.20,
                2023: 0.40,
                2024: 0.30
            },
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
        "base_weight": 0.15,
        "weight_by_month":
            {
                1: 0.05,
                2: 0.04,
                3: 0.05,
                4: 0.06,
                5: 0.07,
                6: 0.10,
                7: 0.12,
                8: 0.15,
                9: 0.13,
                10: 0.11,
                11: 0.07,
                12: 0.05
            },
        "weight_by_year":
            {
                2021: 0.10,
                2022: 0.10,
                2023: 0.35,
                2024: 0.40
            },
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

COUNTRIES = tuple(CITIES_BY_COUNTRY.keys())
CATEGORIES = tuple(CATEGORY_WEIGHTS.keys())

PAYMENT_TYPES = ("Card", "Internet Banking", "UPI", "Wallet")

TXN_FAILURE_RATE = 0.05

PRODUCTS_CSV = './products.csv'

PRODUCTS_DATAFRAME = pd.read_csv(PRODUCTS_CSV)
PRODUCTS_DATAFRAME = PRODUCTS_DATAFRAME[['product_id', 'product_name', 'product_category', 'price']]
PRODUCTS_DATAFRAME['product_name'] = PRODUCTS_DATAFRAME['product_name'].str.replace(r'[^A-Za-z0-9\s]', '', regex=True)

