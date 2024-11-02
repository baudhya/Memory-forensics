import psycopg2
from psycopg2 import sql

# PostgreSQL connection settings
DB_HOST = "localhost"        # Host for Docker setup
DB_PORT = 5432               # Default PostgreSQL port
DB_NAME = "mydatabase"       # Database name from Dockerfile
DB_USER = "myuser"           # User from Dockerfile
DB_PASSWORD = "mypassword"   # Password from Dockerfile

def connect_to_db():
    """Connect to PostgreSQL and return a connection object."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_table(conn):
    """Create a test table in the PostgreSQL database."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INT
            )
        """)
        conn.commit()
        print("Table 'test_table' created successfully.")

def insert_data(conn, name, age):
    """Insert data into the test_table."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO test_table (name, age) VALUES (%s, %s)"),
            [name, age]
        )
        conn.commit()
        print(f"Inserted data: ({name}, {age})")

def fetch_data(conn):
    """Fetch and print all data from test_table."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM test_table")
        rows = cur.fetchall()
        print("Data from 'test_table':")
        for row in rows:
            print(row)

if __name__ == "__main__":
    # Step 1: Connect to the PostgreSQL database
    conn = connect_to_db()

    if conn:
        # Step 2: Create a table
        create_table(conn)
        
        # Step 3: Insert sample data
        insert_data(conn, "Alice", 30)
        insert_data(conn, "Bob", 25)
        
        # Step 4: Fetch and display the data
        fetch_data(conn)
        
        # Step 5: Close the connection
        conn.close()
        print("Database connection closed.")
