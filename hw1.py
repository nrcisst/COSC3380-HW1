
import sys
# Import the psycopg2 (Python driver that connects to psql)

import psycopg2

from db_config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)


# parser.py -- Role A parser
import re
import json
from typing import List, Dict, Tuple

def parse_schema_text(text: str) -> Tuple[List[Dict], List[str]]:
    """
    Parse schema text and return (tables_list, global_errors).
    Each table dict follows the team format.
    """
    tables = []
    global_errors = []
    lineno = 0

    for raw in text.splitlines():
        lineno += 1
        line = raw.strip()
        if not line:
            continue
        if line.startswith('--') or line.startswith('#'):
            continue

        # Expect: TNAME(<cols...>)
        m = re.match(r'^\s*([A-Za-z0-9_]+)\s*\(\s*(.*)\s*\)\s*$', line)
        if not m:
            global_errors.append(f"Line {lineno}: can't parse table line: {raw!r}")
            # create a skip record for safety
            tables.append({
                "table": f"line_{lineno}",
                "pk": None,
                "cols": [],
                "fks": [],
                "composite": True,
                "skip": True,
                "errors": [f"can't parse table line"]
            })
            continue

        tbl = m.group(1).lower()
        inside = m.group(2).strip()
        if inside == "":
            # empty columns -> skip
            tables.append({
                "table": tbl,
                "pk": None,
                "cols": [],
                "fks": [],
                "composite": True,
                "skip": True,
                "errors": ["no columns found"]
            })
            continue

        parts = [p.strip() for p in inside.split(',') if p.strip()]
        cols = []
        fks = []
        pk_candidates = []
        errors = []
        seen = set()

        for part in parts:
            # match column and optional annotation
            mc = re.match(r'^([A-Za-z0-9_]+)\s*(?:\(\s*([^)]+)\s*\))?$', part)
            if not mc:
                errors.append(f"can't parse column token {part!r}")
                continue
            col = mc.group(1).lower()
            ann = mc.group(2)
            if col in seen:
                errors.append(f"duplicate column {col}")
            seen.add(col)
            cols.append(col)

            if ann:
                ann_clean = ann.strip().lower()
                # pk
                if ann_clean == 'pk':
                    pk_candidates.append(col)
                # fk:refTable.refCol or fk refTable.refCol
                elif ann_clean.startswith('fk:') or ann_clean.startswith('fk '):
                    # allow fk:ref or fk ref
                    # split at first ':' or whitespace
                    if ':' in ann_clean:
                        ref = ann_clean.split(':', 1)[1].strip()
                    else:
                        ref = ann_clean.split(None, 1)[1].strip()
                    # ref must be table.col
                    rm = re.match(r'^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$', ref)
                    if not rm:
                        errors.append(f"bad FK reference '{ref}' on column {col}")
                        # keep a placeholder so B/D can see it
                        fks.append({"col": col, "ref_table": None, "ref_pk": None})
                    else:
                        fks.append({"col": col,
                                    "ref_table": rm.group(1).lower(),
                                    "ref_pk": rm.group(2).lower()})
                else:
                    errors.append(f"unknown annotation '{ann_clean}' for column {col}")

        # enforce up to 3 FKs
        if len(fks) > 3:
            errors.append("more than 3 FKs found; only first 3 kept")
            fks = fks[:3]

        pk = None
        composite = False
        if len(pk_candidates) == 1:
            pk = pk_candidates[0]
            composite = False
        else:
            # per teammate spec: 0 or >1 => composite=True and pk=None
            composite = True
            pk = None

        # skip if obvious malformation
        skip = False
        if errors:
            skip = True
        # if pk is declared but not in cols (shouldn't happen), skip
        if pk and pk not in cols:
            errors.append("pk not found in cols")
            skip = True

        tables.append({
            "table": tbl,
            "pk": pk,
            "cols": cols,
            "fks": fks,
            "composite": composite,
            "skip": skip,
            "errors": errors
        })

    return tables, global_errors


def read_schema_file(path: str) -> Tuple[List[Dict], List[str]]:
    with open(path, 'r', encoding='utf-8') as f:
        txt = f.read()
    return parse_schema_text(txt)


def write_parsed_json(schema_path: str, out_path: str = None) -> str:
    """
    Parse schema_path and write parsed JSON to out_path (defaults to schema_path + '.parsed.json').
    Returns out_path used. This function does not print (Role D will report).
    """
    tables, globals_err = read_schema_file(schema_path)
    result = {"tables": tables, "global_errors": globals_err}
    if out_path is None:
        out_path = schema_path + ".parsed.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    return out_path


# Example usage from other modules:
# from parser import read_schema_file
# tables, globals_err = read_schema_file('tc1.txt')

if __name__ == '__main__':
    # If executed directly, write the parsed JSON file (no stdout)
    import sys
    if len(sys.argv) < 2:
        sys.exit(1)
    schema_file = sys.argv[1]
    write_parsed_json(schema_file)
    sys.exit(0)




# Initialize cursor and connection objects
# The "cursor" allows you to execute querys in psql and store its results.
# The "connection" authenticates the imported credentials from db_config.py to establish a connection with the "cursor" to psql.
if __name__ == '__main__' and len(sys.argv) == 1:
    # only do DB stuff if no schema file given
    cursor = None
    connection = None

    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM T0;")
        rows = cursor.fetchall()
        print("Fetched rows:")
        for row in rows:
            col1, col2, col3 = row 
            print(f"col1: {col1}, col2: {col2}, col3: {col3}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# parser runner
if __name__ == '__main__' and len(sys.argv) >= 2:
    schema_file = sys.argv[1]
    write_parsed_json(schema_file)
    print(f"Parsed {schema_file} into {schema_file}.parsed.json")

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

