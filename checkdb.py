import psycopg2

from db_config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

try:
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    
    cursor = connection.cursor()


except Exception as e:
    print(f"An error occurred: {e}")

# After the code succesfully executes make sure to close connections
# Connections can remain open if your program unexpectedly closes
finally:
    if cursor:
        cursor.close()
    
    if connection:
        connection.close()