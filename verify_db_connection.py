import datetime
from db import get_db_connection

def verify_and_create_news_table():
    """
    Verifies the database connection, creates a 'news' table if it doesn't exist,
    and inserts two sample rows.
    """
    conn = None
    try:
        print("Attempting to connect to the database...")
        conn = get_db_connection()
        cursor = conn.cursor()
        print("✅ Successfully connected to the database!")

        # Create the 'news' table
        print("Creating 'news' table (if it doesn't exist)...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """)
        conn.commit()
        print("✅ 'news' table created or already exists.")

        # Insert two random rows
        print("Inserting two random rows into the 'news' table...")
        news_items = [
            ("Breaking News: Gemini CLI is Awesome!", "Users are reporting extreme satisfaction with the new AI assistant.", datetime.datetime.now()),
            ("Tech Update: Python 4.0 Announced", "The Python Software Foundation has just announced the next major version of Python.", datetime.datetime.now())
        ]
        for item in news_items:
            cursor.execute("INSERT INTO news (title, content, created_at) VALUES (%s, %s, %s)", item)
        
        conn.commit()
        print("✅ Successfully inserted two random rows into the 'news' table.")

    except Exception as e:
        print(f"🚨 An error occurred: {e}")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    verify_and_create_news_table()
