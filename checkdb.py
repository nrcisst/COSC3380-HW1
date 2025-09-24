import psycopg2

from db_config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

def check_normalization(conn, table_name, pk, cols):
    """
    Check whether a table is normalized (3NF/BCNF) under simplified rules.

    Inputs:
        conn       : psycopg2 connection object
        table_name : str, name of the table to check
        pk         : str, primary key column name
        cols       : list of str, other columns in the table

    Output:
        'Y' if normalized
        'N' if not normalized
        If composite PK detected, return 'N' with note "case not considered".
    
    Rules (to implement later):
        - For each non-PK column X, pair with each other column Y.
        - Run:
            SELECT COUNT(DISTINCT X) vs. SELECT COUNT(DISTINCT X, Y)
        - If equal AND X repeats, then FD X→Y exists → violation.
        - Append each SQL to checkdb.sql (formatted).
    """
    # TODO: implement SQL checks
    return "N"  # placeholder

# Guard against NameError in finally if connect fails
connection = None
cursor = None

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