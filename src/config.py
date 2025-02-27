"""
Configuration parameters for the data generation process.
"""

# Customer data parameters
COUNTRY_FILL_RATE = 0.95
DOB_FILL_RATE = 0.50
EMAIL_FILL_RATE = 0.80
PHONE_FILL_RATE = 0.75

# Configurazione del numero di record
NUM_CUSTOMERS = 1_000_000
NUM_ORDERS = 2_000_000

# Configurazione delle dimensioni dei batch
CUSTOMER_BATCH_SIZE = 50_000
ORDER_BATCH_SIZE = 100_000
TRANSACTION_BATCH_SIZE = 100_000

# Database configuration
DB_FILE = "../data/duckdb_demo.duckdb"

# Domini email popolari globali e specifici per paese con pesi relativi
EMAIL_DOMAINS = {
    # Domini globali (peso maggiore)
    "gmail.com": 0.25,
    "outlook.com": 0.15,
    "yahoo.com": 0.10,
    "hotmail.com": 0.08,
    "icloud.com": 0.07,
    "protonmail.com": 0.03,
    "mail.com": 0.02,
    "aol.com": 0.02,
    "zoho.com": 0.01,
    "yandex.com": 0.01,
    "gmx.com": 0.01,
    
    # Domini specifici per paese (peso minore)
    # USA
    "comcast.net": 0.01,
    "verizon.net": 0.01,
    "att.net": 0.01,
    # UK
    "btinternet.com": 0.005,
    "sky.com": 0.005,
    "virginmedia.com": 0.005,
    # Germania
    "web.de": 0.005,
    "t-online.de": 0.005,
    "freenet.de": 0.005,
    # Francia
    "orange.fr": 0.005,
    "free.fr": 0.005,
    "sfr.fr": 0.005,
    # Italia
    "libero.it": 0.005,
    "tiscali.it": 0.005,
    "virgilio.it": 0.005,
    # Spagna
    "telefonica.es": 0.005,
    "movistar.es": 0.005,
    # Giappone
    "docomo.ne.jp": 0.005,
    "ezweb.ne.jp": 0.005,
    # Cina
    "qq.com": 0.01,
    "163.com": 0.01,
    "126.com": 0.005,
    # India
    "rediffmail.com": 0.005,
    "indiatimes.com": 0.005,
    # Brasile
    "uol.com.br": 0.005,
    "bol.com.br": 0.005,
    # Russia
    "mail.ru": 0.01,
    "rambler.ru": 0.005,
    # Canada
    "rogers.com": 0.005,
    "shaw.ca": 0.005,
    # Australia
    "bigpond.com": 0.005,
    "optusnet.com.au": 0.005,
    # Altre regioni
    "telkom.net": 0.003,
    "singnet.com.sg": 0.003,
    "sympatico.ca": 0.003,
    "naver.com": 0.003,
    "wanadoo.fr": 0.003,
    "bluewin.ch": 0.002
}

# Formati data di nascita con pesi relativi
DOB_FORMATS = {
    "%Y-%m-%d": 0.5,  # 1980-12-25
    "%d/%m/%Y": 0.25, # 25/12/1980
    "%m/%d/%Y": 0.15, # 12/25/1980 (formato USA)
    "%Y/%m/%d": 0.05, # 1980/12/25
    "%d-%m-%Y": 0.05  # 25-12-1980
}

# Customer source distribution
CUSTOMER_SOURCES = {
    1: 0.4,  # E-commerce (40%)
    2: 0.6   # POS (60%)
}

# Transaction source distribution
TRANSACTION_SOURCES = {
    1: 0.5,  # E-commerce (50%)
    2: 0.5   # POS (50%)
}

# Customer duplication settings
CUSTOMER_DUPLICATION_RATE = 0.2  # 20% dei clienti sono duplicati
EMAIL_PHONE_MATCH_RATE = 0.8     # 80% dei duplicati sono per email/telefono
NAME_MATCH_WITH_TYPO_RATE = 0.5  # 50% dei match per nome hanno typo

# Date range for transactions
TRANSACTION_START_DATE = "2023-01-01"
TRANSACTION_END_DATE = "2025-02-25"  # Current date

# Product categories and their weight distribution
PRODUCT_CATEGORIES = {
    "Fruits & Vegetables": 0.15,
    "Dairy & Eggs": 0.13,
    "Meat & Seafood": 0.12,
    "Bakery": 0.10,
    "Beverages": 0.09,
    "Snacks & Candy": 0.08,
    "Frozen Foods": 0.07,
    "Canned Goods": 0.06,
    "Dry Goods & Pasta": 0.05,
    "Condiments & Sauces": 0.04,
    "Breakfast Foods": 0.03,
    "Health & Beauty": 0.03,
    "Cleaning Supplies": 0.03,
    "Baby Products": 0.01,
    "Pet Supplies": 0.01,
}

# Product price ranges by category (min, max in monetary units)
PRODUCT_PRICE_RANGES = {
    "Fruits & Vegetables": (0.5, 10.0),
    "Dairy & Eggs": (1.0, 8.0),
    "Meat & Seafood": (5.0, 30.0),
    "Bakery": (1.0, 15.0),
    "Beverages": (0.8, 20.0),
    "Snacks & Candy": (0.5, 8.0),
    "Frozen Foods": (2.0, 15.0),
    "Canned Goods": (0.8, 5.0),
    "Dry Goods & Pasta": (0.5, 10.0),
    "Condiments & Sauces": (1.0, 8.0),
    "Breakfast Foods": (2.0, 10.0),
    "Health & Beauty": (3.0, 50.0),
    "Cleaning Supplies": (2.0, 20.0),
    "Baby Products": (5.0, 30.0),
    "Pet Supplies": (2.0, 40.0),
}

# Common products by category
PRODUCTS_BY_CATEGORY = {
    "Fruits & Vegetables": [
        "Apples", "Bananas", "Oranges", "Strawberries", "Blueberries", 
        "Tomatoes", "Carrots", "Lettuce", "Broccoli", "Spinach", 
        "Potatoes", "Onions", "Peppers", "Cucumbers", "Avocados"
    ],
    "Dairy & Eggs": [
        "Milk", "Eggs", "Butter", "Cheese", "Yogurt", 
        "Cream", "Sour Cream", "Cottage Cheese", "Ice Cream", "Cream Cheese"
    ],
    "Meat & Seafood": [
        "Chicken Breast", "Ground Beef", "Steak", "Pork Chops", "Bacon", 
        "Salmon", "Tuna", "Shrimp", "Sausages", "Ham"
    ],
    "Bakery": [
        "Bread", "Bagels", "Muffins", "Croissants", "Cakes", 
        "Cookies", "Pies", "Donuts", "Baguette", "Rolls"
    ],
    "Beverages": [
        "Water", "Coffee", "Tea", "Soda", "Juice", 
        "Beer", "Wine", "Milk", "Energy Drinks", "Sports Drinks"
    ],
    "Snacks & Candy": [
        "Chips", "Pretzels", "Crackers", "Popcorn", "Nuts", 
        "Chocolate", "Candy", "Granola Bars", "Cookies", "Fruit Snacks"
    ],
    "Frozen Foods": [
        "Pizza", "Ice Cream", "Frozen Vegetables", "Frozen Meals", "Frozen Fruit", 
        "Frozen Appetizers", "Frozen Breakfast", "Frozen Desserts", "Frozen Fish", "Frozen Chicken"
    ],
    "Canned Goods": [
        "Canned Tuna", "Canned Soup", "Canned Beans", "Canned Vegetables", "Canned Fruit", 
        "Canned Tomatoes", "Canned Corn", "Canned Chili", "Canned Meat", "Canned Sauce"
    ],
    "Dry Goods & Pasta": [
        "Pasta", "Rice", "Cereal", "Flour", "Sugar", 
        "Beans", "Lentils", "Quinoa", "Oats", "Pancake Mix"
    ],
    "Condiments & Sauces": [
        "Ketchup", "Mustard", "Mayonnaise", "Salad Dressing", "Olive Oil", 
        "Vinegar", "Soy Sauce", "Hot Sauce", "BBQ Sauce", "Pasta Sauce"
    ],
    "Breakfast Foods": [
        "Cereal", "Oatmeal", "Pancake Mix", "Waffles", "Breakfast Bars", 
        "Syrup", "Breakfast Sandwich", "Bagels", "Muffins", "Granola"
    ],
    "Health & Beauty": [
        "Shampoo", "Conditioner", "Soap", "Toothpaste", "Deodorant", 
        "Lotion", "Facial Cleanser", "Tissues", "Razors", "Vitamins"
    ],
    "Cleaning Supplies": [
        "Laundry Detergent", "Dish Soap", "All-Purpose Cleaner", "Paper Towels", "Toilet Paper", 
        "Sponges", "Garbage Bags", "Window Cleaner", "Bleach", "Disinfectant Wipes"
    ],
    "Baby Products": [
        "Diapers", "Baby Wipes", "Baby Food", "Formula", "Baby Shampoo", 
        "Baby Lotion", "Baby Powder", "Baby Oil", "Baby Toys", "Baby Clothes"
    ],
    "Pet Supplies": [
        "Dog Food", "Cat Food", "Pet Treats", "Pet Toys", "Cat Litter", 
        "Pet Shampoo", "Pet Beds", "Pet Bowls", "Pet Medication", "Pet Accessories"
    ],
}

# Country GDP and income per capita weights (aggiornati in base al GDP relativo)
# I valori sono proporzionali al GDP, con USA=1.0 come riferimento
COUNTRY_GDP_WEIGHTS = {
    "United States": 1.0,
    "China": 0.70,
    "Japan": 0.23,
    "Germany": 0.19,
    "United Kingdom": 0.13,
    "India": 0.13,
    "France": 0.13,
    "Italy": 0.10,
    "Canada": 0.08,
    "South Korea": 0.08,
    "Russia": 0.08,
    "Brazil": 0.08,
    "Australia": 0.07,
    "Spain": 0.07,
    "Mexico": 0.06,
    "Indonesia": 0.05,
    "Netherlands": 0.05,
    "Saudi Arabia": 0.04,
    "Turkey": 0.04,
    "Switzerland": 0.03,
    "Taiwan": 0.03,
    "Poland": 0.03,
    "Sweden": 0.03,
    "Belgium": 0.02,
    "Thailand": 0.02,
    "Ireland": 0.02,
    "Austria": 0.02,
    "Nigeria": 0.02,
    "Israel": 0.02,
    "Singapore": 0.02,
    "Hong Kong": 0.02,
    "Malaysia": 0.02,
    "Denmark": 0.02,
    "South Africa": 0.02,
    "Philippines": 0.02,
    "Pakistan": 0.01,
    "UAE": 0.01,
    "Norway": 0.01,
    "Czechia": 0.01,
    "Bangladesh": 0.01,
    "Finland": 0.01,
    "New Zealand": 0.01,
}

# Income per capita factors (per adeguare i prezzi in base al potere d'acquisto)
# I valori sono proporzionali all'income per capita, con USA=1.0 come riferimento
INCOME_PER_CAPITA_FACTORS = {
    "United States": 1.00,
    "Switzerland": 1.12,
    "Norway": 1.10,
    "Singapore": 1.05,
    "Ireland": 1.05,
    "Denmark": 1.02,
    "Australia": 0.95,
    "Hong Kong": 0.95,
    "Netherlands": 0.92,
    "Sweden": 0.92,
    "Germany": 0.90,
    "Belgium": 0.88,
    "Canada": 0.87,
    "Austria": 0.87,
    "Finland": 0.84,
    "United Kingdom": 0.83,
    "France": 0.82,
    "Japan": 0.81,
    "New Zealand": 0.80,
    "Israel": 0.75,
    "Italy": 0.72,
    "South Korea": 0.70,
    "Spain": 0.65,
    "Taiwan": 0.60,
    "Czechia": 0.55,
    "Poland": 0.50,
    "Malaysia": 0.48,
    "China": 0.45,
    "Russia": 0.45,
    "Turkey": 0.40,
    "Mexico": 0.36,
    "Brazil": 0.34,
    "Thailand": 0.32,
    "South Africa": 0.30,
    "Indonesia": 0.25,
    "Philippines": 0.20,
    "India": 0.18,
    "Nigeria": 0.15,
    "Pakistan": 0.14,
    "Bangladesh": 0.13,
    "UAE": 0.80,
    "Saudi Arabia": 0.75,
}

# Country population and purchasing power weights
COUNTRY_WEIGHTS = COUNTRY_GDP_WEIGHTS

# ISO country codes mapping (for formatted output options)
ISO_COUNTRY_CODES = {
    "China": "CN",
    "India": "IN",
    "United States": "US",
    "Indonesia": "ID",
    "Pakistan": "PK",
    "Brazil": "BR",
    "Nigeria": "NG",
    "Bangladesh": "BD",
    "Russia": "RU",
    "Mexico": "MX",
    "Japan": "JP",
    "Germany": "DE",
    "France": "FR",
    "United Kingdom": "GB",
    "Italy": "IT",
    "South Korea": "KR",
    "Spain": "ES",
    "Canada": "CA",
    "Saudi Arabia": "SA",
    "Australia": "AU",
    "South Africa": "ZA",
    "Turkey": "TR",
    "Argentina": "AR",
    "Netherlands": "NL",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Belgium": "BE",
    "Austria": "AT",
    "Ireland": "IE",
    "Singapore": "SG",
    "New Zealand": "NZ",
    "UAE": "AE",
    "Finland": "FI",
    "Norway": "NO",
    "Denmark": "DK",
}

# Native country names
NATIVE_COUNTRY_NAMES = {
    "China": "中国",
    "India": "भारत",
    "United States": "United States",
    "Indonesia": "Indonesia",
    "Pakistan": "پاکستان",
    "Brazil": "Brasil",
    "Nigeria": "Nigeria",
    "Bangladesh": "বাংলাদেশ",
    "Russia": "Россия",
    "Mexico": "México",
    "Japan": "日本",
    "Germany": "Deutschland",
    "France": "France",
    "United Kingdom": "United Kingdom",
    "Italy": "Italia",
    "South Korea": "대한민국",
    "Spain": "España",
    "Canada": "Canada",
    "Saudi Arabia": "المملكة العربية السعودية",
    "Australia": "Australia",
    "South Africa": "South Africa",
    "Turkey": "Türkiye",
    "Argentina": "Argentina",
    "Netherlands": "Nederland",
    "Sweden": "Sverige",
    "Switzerland": "Schweiz",
    "Belgium": "België",
    "Austria": "Österreich",
    "Ireland": "Ireland",
    "Singapore": "Singapore",
    "New Zealand": "New Zealand",
    "UAE": "الإمارات العربية المتحدة",
    "Finland": "Suomi",
    "Norway": "Norge",
    "Denmark": "Danmark",
}
