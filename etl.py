import os
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()

RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')

host= os.getenv('HOST')
user= os.getenv('MYSQL_USERNAME')
password= os.getenv('MYSQL_PASSWORD')
database= os.getenv('MYSQL_DATABASE')

params = {"category":"videogames", "type":"BEST_SELLERS","page":"1","country":"US"}

headers = {
    "x-rapidapi-key": RAPIDAPI_KEY,
	"x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

url = "https://real-time-amazon-data.p.rapidapi.com/best-sellers"

def check_rate_limits():
    """
    Check the API quota allocated to your account
    """
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    daily_limits = response.headers.get('x-ratelimit-requests-limit')
    daily_remaining = response.headers.get('x-ratelimit-requests-remaining')
    calls_per_min_allowed = response.headers.get('X-RateLimit-Limit')
    calls_per_min_remaining = response.headers.get('X-RateLimit-Remaining')

    rate_limits = {
        'daily_limit': daily_limits,
        'daily_remaining': daily_remaining,
        'minute_limit': calls_per_min_allowed,
        'minute_remaining': calls_per_min_remaining
    }

    print(rate_limits)


def get_products(url, headers, params):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        print(response.json())
        return response.json()
    
    except requests.exceptions.HTTPError as http_error_message:
        print(f"[HTTP ERROR]: {http_error_message}")
    
    except requests.exceptions.ConnectionError as connection_error_message:
        print(f"[CONNECTION ERROR]: {connection_error_message}")
    
    except requests.exceptions.Timeout as timeout_error_message:
        print(f"[TIMEOUT ERROR]: {timeout_error_message}")
    
    except requests.exceptions.RequestException as other_error_message:
        print(f"[UNKNOWN ERROR]: {other_error_message}")

def process_products(data):
    products = []
    for products_data in data['data']['best_sellers']:
        title = products_data['product_title']
        price = products_data['product_price']
        star_rating = products_data['product_star_rating']
        num_ratings = products_data['product_num_ratings']
        url = products_data['product_url']
        picture = products_data['product_photo']
        rank_change_label = products_data['rank_change_label']

        products.append({
            'product_title': title,
            'product_price': price,
            'product_star_rating': float(star_rating) if star_rating is not None else 0.0,
            'product_num_ratings': int(num_ratings) if num_ratings is not None else 0,
            'product_url': url,
            'product_picture': picture,
            'product_rank_change_label': rank_change_label
        })
    return products

def create_dataframe(products):
    df = pd.DataFrame(products)

    return df

def create_db_connection(host_name, user_name, user_password, db_name):
    db_connection = None

    try:
        db_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    
    except Error as e:
        print(f"[DATABASE CONNECTION ERROR]: '{e}'")
    
    return db_connection

def create_table(db_connection):
    CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS amazon_products(
        `product_title` VARCHAR(255),
        `product_price` VARCHAR(255),
        `product_star_rating` VARCHAR(255),
        `product_num_ratings` INT,
        `product_url` VARCHAR(255),
        `product_picture` VARCHAR(255),
        `product_rank_change_label` VARCHAR(255),
        PRIMARY KEY(`product_url`)
    );
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_QUERY)
        db_connection.commit()
        print("Table created successfully")
    except Error as e:
        print(f"[CREATING TABLE ERROR]: '{e}'")

def insert_into_table(db_connection, df):
    cursor = db_connection.cursor()

    INSERT_DATA_SQL_QUERY = """
    INSERT INTO amazon_products (`product_title`, `product_price`, `product_star_rating`, `product_num_ratings`, `product_url`, `product_picture`, `product_rank_change_label`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `product_price` = VALUES(`product_price`),
        `product_star_rating` = VALUES(`product_star_rating`),
        `product_num_ratings` = VALUES(`product_num_ratings`),
        `product_rank_change_label` = VALUES(`product_rank_change_label`)
    """

    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]

    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()
    print("Data inserted or updated successfully")

def run_data_pipeline():
    check_rate_limits()

    data = get_products(url, headers, params)

    if data and 'data' in data and 'best_sellers' in data['data'] and data['data']['best_sellers']:
        products = process_products(data)
        df = create_dataframe(products)
        print(df.to_string(index=False))
    else:
        print("⚠ No data available or an error occurred.")
        return 

    # Connect to the database
    db_connection = create_db_connection(host, user, password, database)

    if db_connection is not None:
        create_table(db_connection)
        insert_into_table(db_connection, df) 
    else:
        print("⚠ Database connection failed.")


