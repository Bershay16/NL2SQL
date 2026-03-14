
import sqlite3

def create_sample_db():
    conn = sqlite3.connect('sample.db')
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT UNIQUE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        item_name TEXT,
        price REAL,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Sample database 'sample.db' created.")

if __name__ == "__main__":
    create_sample_db()
