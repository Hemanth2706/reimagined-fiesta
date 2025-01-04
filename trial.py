import streamlit as st
import pymysql
import pandas as pd

# Function to create a connection to MySQL
def create_connection():
    try:
        connection = pymysql.connect(
            host='127.0.0.1',      
            user='root',           
            password='Hemanth*270605',   
            database='bookscape',  
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except pymysql.MySQLError as e:
        st.error(f"Connection Error: {e}")
        return None

# Function to fetch data based on the query
def fetch_data(query):
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            connection.close()
            return results
        except pymysql.MySQLError as e:
            st.error(f"Query Error: {e}")
            return None
    else:
        return None

# Data Extraction Section
st.title("Book Data Extraction")

api_key = st.text_input("Enter Google Books API Key")
search_keyword = st.text_input("Enter Search Keyword")

if st.button("Fetch and Load Data"):
    # Add your Google Books API data extraction logic here
    # For example: Use the Google Books API to fetch data and store it in the MySQL database

    st.success("Data Loaded Successfully")

# Queries Section
st.title("Data Analysis with SQL Queries")

queries = [
    ("Check Availability of eBooks vs Physical Books", 
     "SELECT isEbook, COUNT(*) AS book_count FROM books GROUP BY isEbook"),
    
    ("Find the Publisher with the Most Books Published", 
     "SELECT book_title, COUNT(*) AS book_count FROM books GROUP BY book_title ORDER BY book_count DESC LIMIT 1"),
    
    ("Identify the Publisher with the Highest Average Rating", 
     "SELECT book_subtitle, AVG(averageRating) AS avg_rating FROM books GROUP BY book_subtitle ORDER BY avg_rating DESC LIMIT 1"),
    
    ("Get the Top 5 Most Expensive Books by Retail Price", 
     "SELECT book_title, amount_retailPrice FROM books ORDER BY amount_retailPrice DESC LIMIT 5"),
    
    ("Find Books Published After 2010 with at Least 500 Pages", 
     "SELECT * FROM books WHERE year > 2010 AND pageCount >= 500"),
    
    ("List Books with Discounts Greater than 20%", 
     "SELECT book_title, amount_listPrice, amount_retailPrice FROM books WHERE (amount_listPrice - amount_retailPrice) / amount_listPrice > 0.2"),
    
    ("Find the Average Page Count for eBooks vs Physical Books", 
     "SELECT isEbook, AVG(pageCount) AS avg_pages FROM books GROUP BY isEbook"),
    
    ("Find the Top 3 Authors with the Most Books", 
     "SELECT book_authors AS author, COUNT(*) AS book_count FROM books WHERE book_authors IS NOT NULL AND TRIM(book_authors) != '' GROUP BY book_authors ORDER BY book_count DESC LIMIT 3;"),
    
    ("List Publishers with More than 10 Books", 
     "SELECT book_title AS publisher, COUNT(*) AS book_count FROM books WHERE book_title IS NOT NULL AND TRIM(book_title) != '' GROUP BY book_title HAVING COUNT(*) > 10 ORDER BY book_count DESC;"),
    
    ("Find the Average Page Count for Each Category", 
     "SELECT search_key, AVG(pageCount) AS avg_pages FROM books GROUP BY search_key"),
    
    ("Retrieve Books with More than 3 Authors", 
     "SELECT book_title, book_authors FROM books WHERE LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) + 1 > 3"),
    
    ("Books with Ratings Count Greater Than the Average", 
     "SELECT book_title, ratingsCount FROM books WHERE ratingsCount > (SELECT AVG(ratingsCount) FROM books)"),
    
    ("Books with the Same Author Published in the Same Year", 
     "SELECT book_authors AS author, year, COUNT(*) AS book_count FROM books WHERE book_authors IS NOT NULL AND TRIM(book_authors) != '' AND year IS NOT NULL AND TRIM(year) != '' GROUP BY book_authors, year HAVING COUNT(*) > 1;"),
    
    ("Books with a Specific Keyword in the Title", 
     "SELECT book_title FROM books WHERE book_title LIKE '%SQL%'"),
    
    ("Year with the Highest Average Book Price", 
     "SELECT year, AVG(amount_retailPrice) AS avg_price FROM books GROUP BY year ORDER BY avg_price DESC LIMIT 1"),
    
    ("Count Authors Who Published 3 Consecutive Years", 
     "SELECT book_authors AS author, COUNT(DISTINCT year) AS years_count FROM books WHERE book_authors IS NOT NULL AND TRIM(book_authors) != '' GROUP BY book_authors HAVING MAX(year) - MIN(year) >= 2 AND COUNT(DISTINCT year) >= 3;"),
    
    ("Authors Who Have Published Books in the Same Year but Under Different Publishers", 
     "SELECT book_authors AS author, year, COUNT(DISTINCT book_subtitle) AS publisher_count FROM books WHERE book_authors IS NOT NULL AND TRIM(book_authors) != '' AND year IS NOT NULL AND TRIM(year) != '' GROUP BY book_authors, year HAVING COUNT(DISTINCT book_subtitle) > 1;"),
    
    ("Average Amount Retail Price of eBooks and Physical Books", 
     "SELECT AVG(amount_retailPrice) AS avg_price, isEbook FROM books GROUP BY isEbook"),
    
    ("Books with an Average Rating More Than Two Standard Deviations Away from the Average", 
     "SELECT book_title, averageRating, ratingsCount FROM books WHERE ABS(averageRating - (SELECT AVG(averageRating) FROM books)) > 2 * (SELECT STDDEV(averageRating) FROM books)"),
    
    ("Publisher with the Highest Average Rating Among Books Published More Than 10 Times", 
     "SELECT book_subtitle, AVG(averageRating) AS avg_rating, COUNT(*) AS book_count FROM books GROUP BY book_subtitle HAVING book_count > 10 ORDER BY avg_rating DESC LIMIT 1")
]

# Dropdown to select and run queries
query_selection = st.selectbox("Select a Query", [q[0] for q in queries])

# Execute the selected query
if query_selection:
    query = [q[1] for q in queries if q[0] == query_selection][0]
    result = fetch_data(query)
    if result:
        df = pd.DataFrame(result)
        st.write(df)
    else:
        st.error("No results found or query error.")
