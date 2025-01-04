#!/usr/bin/env python
# coding: utf-8

# In[12]:


get_ipython().system('pip install mysql-connector-python')


# In[2]:


get_ipython().system('pip install pymysql')


# In[12]:


import pymysql
import requests
import time

# MySQL Configuration
MYSQL_HOST = "127.0.0.1"         # Your MySQL host (e.g., "127.0.0.1")
MYSQL_USER = "root"     # Your MySQL username
MYSQL_PASSWORD = "Hemanth*270605" # Your MySQL password
MYSQL_DATABASE = "bookscape"     # Your database name

# API Configuration
API_KEY = "AIzaSyA61my85cb5R5gvvq14hfbeN2_HHgv4qwA"  # Replace with your API key
BASE_URL = "https://www.googleapis.com/books/v1/volumes"

# Function to connect to MySQL
def connect_to_mysql():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

# Function to create the books table in MySQL
def create_books_table():
    conn = connect_to_mysql()
    cursor = conn.cursor()

    # Create table query
    create_table_query = """
    CREATE TABLE IF NOT EXISTS books (
        book_id VARCHAR(255) PRIMARY KEY,
        search_key VARCHAR(255),
        book_title VARCHAR(255),
        book_subtitle TEXT,
        book_authors TEXT,
        book_description TEXT,
        industryIdentifiers TEXT,
        text_readingModes BOOLEAN,
        image_readingModes BOOLEAN,
        pageCount INT,
        categories TEXT,
        language VARCHAR(10),
        imageLinks TEXT,
        ratingsCount INT,
        averageRating DECIMAL(3, 2),
        country VARCHAR(10),
        saleability VARCHAR(50),
        isEbook BOOLEAN,
        amount_listPrice DECIMAL(10, 2),
        currencyCode_listPrice VARCHAR(10),
        amount_retailPrice DECIMAL(10, 2),
        currencyCode_retailPrice VARCHAR(10),
        buyLink TEXT,
        year VARCHAR(10)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()
    print("Books table created successfully!")

# Function to fetch books from Google Books API
def fetch_books(query, max_results=500):
    books = []
    start_index = 0
    results_per_request = 40  # Maximum results per API request

    while len(books) < max_results:
        params = {
            "q": query,
            "startIndex": start_index,
            "maxResults": min(results_per_request, max_results - len(books)),
            "key": API_KEY
        }

        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            books.extend(items)

            if len(items) < results_per_request:
                break  # No more results to fetch
            start_index += results_per_request
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break

        # Handle API rate limits
        time.sleep(1)

    return books

# Function to store books data in MySQL
def store_books_in_mysql(books, query):
    conn = connect_to_mysql()
    cursor = conn.cursor()

    # Insert data into the books table
    insert_query = """
    INSERT IGNORE INTO books (
        book_id, search_key, book_title, book_subtitle, book_authors, book_description,
        industryIdentifiers, text_readingModes, image_readingModes, pageCount,
        categories, language, imageLinks, ratingsCount, averageRating, country,
        saleability, isEbook, amount_listPrice, currencyCode_listPrice,
        amount_retailPrice, currencyCode_retailPrice, buyLink, year
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for book in books:
        volume_info = book.get("volumeInfo", {})
        sale_info = book.get("saleInfo", {})

        # Extract book details
        book_id = book.get("id")
        title = volume_info.get("title", "Unknown")
        subtitle = volume_info.get("subtitle", "")
        authors = ", ".join(volume_info.get("authors", []))
        description = volume_info.get("description", "")
        industry_identifiers = str(volume_info.get("industryIdentifiers", []))
        text_mode = volume_info.get("readingModes", {}).get("text", False)
        image_mode = volume_info.get("readingModes", {}).get("image", False)
        page_count = volume_info.get("pageCount", 0)
        categories = ", ".join(volume_info.get("categories", []))
        language = volume_info.get("language", "Unknown")
        image_links = str(volume_info.get("imageLinks", {}))
        ratings_count = volume_info.get("ratingsCount", 0)
        average_rating = volume_info.get("averageRating", 0)
        country = sale_info.get("country", "Unknown")
        saleability = sale_info.get("saleability", "Unknown")
        is_ebook = sale_info.get("isEbook", False)
        list_price = sale_info.get("listPrice", {}).get("amount", 0)
        list_price_currency = sale_info.get("listPrice", {}).get("currencyCode", "")
        retail_price = sale_info.get("retailPrice", {}).get("amount", 0)
        retail_price_currency = sale_info.get("retailPrice", {}).get("currencyCode", "")
        buy_link = sale_info.get("buyLink", "")
        year = volume_info.get("publishedDate", "Unknown")

        cursor.execute(insert_query, (
            book_id, query, title, subtitle, authors, description, industry_identifiers, text_mode,
            image_mode, page_count, categories, language, image_links, ratings_count,
            average_rating, country, saleability, is_ebook, list_price, list_price_currency,
            retail_price, retail_price_currency, buy_link, year
        ))

    conn.commit()
    conn.close()
    print("Books data inserted successfully!")

# Main function
if __name__ == "__main__":
    search_query = "microsoft azure"  # Replace with your search query
    print("Fetching book data...")
    books_data = fetch_books(search_query, max_results=500)

    print("Creating books table...")
    create_books_table()

    print("Storing data in MySQL database...")
    store_books_in_mysql(books_data, search_query)

    print("Data extraction and storage completed.")


# In[12]:


import pymysql

# Establishing connection to the database
try:
    conn = pymysql.connect(
        host="127.0.0.1",       # Replace with your host
        user="root",            # Replace with your username
        password="Hemanth*270605",  # Replace with your password
        database="bookscape"    # Replace with your database name
    )
    
    # Check if connection is open
    if conn.open:
        print("Connected to MySQL database")
    
    # Create a cursor object for executing queries
    cursor = conn.cursor()

except pymysql.MySQLError as e:
    print(f"Error: {e}")




# In[38]:


get_ipython().system('pip install streamlit pymysql pandas matplotlib seaborn')


# In[ ]:





# In[ ]:




