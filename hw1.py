
import sys
import re, os
from typing import List, Dict, Tuple
from datetime import datetime
from pathlib import Path
import psycopg2

from db_config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

# -----------------------------
# Role A: schema parser

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
                "errors": ["can't parse table line"]
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
                        fks.append({
                            "col": col,
                            "ref_table": rm.group(1).lower(),
                            "ref_pk": rm.group(2).lower()
                        })
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

        # skip only if unrecoverable malformation
        skip = False
        if pk and pk not in cols:
            errors.append("pk not found in cols")
            skip = True
        if pk is None:  # no valid PK (0 or >1 PKs)
            skip = True
        if not cols:
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

# -----------------------------
# Role B: RI checker (autolog to Role B section)

RI_SQL_FILE = "checkdb.sql"
RI_SQL_LOG_BUFFER = []

def log_ri_sql(sql: str):
    RI_SQL_LOG_BUFFER.append(sql.strip().rstrip(";") + ";\n")

def flush_ri_sql_to_checkdb(testcase_name: str):
    if not RI_SQL_LOG_BUFFER:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    block = (
        f"\n-- ----- testcase: {testcase_name} (Role B, {ts}) -----\n"
        + "".join(RI_SQL_LOG_BUFFER) + "\n"
    )
    p = Path(RI_SQL_FILE)
    if not p.exists():
        p.write_text(
            "-- =====================================\n-- checkdb.sql\n-- =====================================\n\n"
            "-- =====================================\n-- Role A: Parser\n-- =====================================\n\n"
            "-- =====================================\n-- Role B: Referential Integrity\n-- =====================================\n"
            "-- >>> ROLE B AUTOLOG ANCHOR (do not remove) <<<\n\n"
            "-- =====================================\n-- Role C: Normalization\n-- =====================================\n"
            "-- >>> ROLE C AUTOLOG ANCHOR (do not remove) <<<\n\n"
            "-- =====================================\n-- Role D: Output / Orchestration\n-- =====================================\n",
            encoding="utf-8"
        )
    text = p.read_text(encoding="utf-8")
    anchor = "-- >>> ROLE B AUTOLOG ANCHOR (do not remove) <<<"
    idx = text.find(anchor)
    if idx == -1:
        text = text.rstrip() + block
    else:
        insert_at = idx + len(anchor)
        text = text[:insert_at] + block + text[insert_at:]
    p.write_text(text, encoding="utf-8")
    RI_SQL_LOG_BUFFER.clear()


def check_referential_integrity(conn, table_name: str, fks: list) -> str:

    cur = conn.cursor()
    cur.execute("SET search_path TO public;")

    all_ok = True
    for fk in fks:
        col = fk.get("col")
        rt  = fk.get("ref_table")
        rp  = fk.get("ref_pk")
        # Skip malformed FK specs gracefully
        if not col or not rt or not rp:
            all_ok = False
            continue

        # 1) Violations query (preferred & clear): orphan rows where fk is NOT NULL but no matching parent
        q_viol = (
            f'SELECT COUNT(*) AS fk_violations '
            f'FROM public.{table_name} child '
            f'LEFT JOIN public.{rt} parent ON child.{col} = parent.{rp} '
            f'WHERE child.{col} IS NOT NULL AND parent.{rp} IS NULL'
        )
        cur.execute(q_viol)
        (violations,) = cur.fetchone()
        log_ri_sql(q_viol)

        # 2) (Optional) Equality check for intuition (counts equal iff 0 violations):
        q_count_all = f"SELECT COUNT(*) FROM public.{table_name}"
        q_count_join = (
            f"SELECT COUNT(*) FROM public.{table_name} c "
            f"JOIN public.{rt} p ON c.{col} = p.{rp}"
        )
        cur.execute(q_count_all); (cnt_all,)  = cur.fetchone(); log_ri_sql(q_count_all)
        cur.execute(q_count_join); (cnt_join,) = cur.fetchone(); log_ri_sql(q_count_join)

        # Decide pass/fail for this FK
        if violations != 0:
            all_ok = False

    return "Y" if all_ok else "N"

# -----------------------------
# Normalization checker
SQL_FILE = "checkdb.sql"
SQL_LOG_BUFFER = []

def log_norm_sql(sql: str):
    SQL_LOG_BUFFER.append(sql.strip().rstrip(";") + ";\n")

def flush_norm_sql_to_checkdb(testcase_name: str):
    if not SQL_LOG_BUFFER:
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    block = (
        f"\n-- ----- testcase: {testcase_name} (Role C, {ts}) -----\n"
        + "".join(SQL_LOG_BUFFER) + "\n"
    )

    p = Path(SQL_FILE)
    if not p.exists():
        # minimal skeleton if file missing
        p.write_text(
            "-- =====================================\n-- checkdb.sql\n-- =====================================\n\n"
            "-- =====================================\n-- Role A: Parser\n-- =====================================\n\n"
            "-- =====================================\n-- Role B: Referential Integrity\n-- =====================================\n\n"
            "-- =====================================\n-- Role C: Normalization\n-- =====================================\n"
            "-- >>> ROLE C AUTOLOG ANCHOR (do not remove) <<<\n\n"
            "-- =====================================\n-- Role D: Output / Orchestration\n-- =====================================\n",
            encoding="utf-8"
        )

    text = p.read_text(encoding="utf-8")
    anchor = "-- >>> ROLE C AUTOLOG ANCHOR (do not remove) <<<"
    idx = text.find(anchor)
    if idx == -1:
        # if someone removed the anchor, just append at end (still safe)
        text = text.rstrip() + block
    else:
        insert_at = idx + len(anchor)
        text = text[:insert_at] + block + text[insert_at:]

    p.write_text(text, encoding="utf-8")
    SQL_LOG_BUFFER.clear()

def run_and_log(cur, sql: str):
    cur.execute(sql)
    log_norm_sql(sql)
    return cur.fetchone()

def check_normalization(conn, table_name, pk, cols):
    cur = conn.cursor()
    ok = True
    non_pk = [c for c in cols if c != pk]

    for x in non_pk:
        # does X repeat?
        n, dx = run_and_log(cur, f"SELECT COUNT(*), COUNT(DISTINCT {x}) FROM {table_name};")
        if dx == n:
            continue

        # test FD X -> Y
        for y in non_pk:
            if y == x:
                continue
            dxy, dx2 = run_and_log(cur,
                f"SELECT COUNT(DISTINCT ({x},{y})), COUNT(DISTINCT {x}) FROM {table_name};"
            )
            if dxy == dx2:
                ok = False
                break
        if not ok:
            break

    return "Y" if ok else "N"


# Single entrypoint
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        schema_file = sys.argv[1]
        tables, _ = read_schema_file(schema_file)  # parse schema text

        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )

        for t in tables:
            if t["skip"]:
                continue

            # Role B: Referential Integrity
            if t["fks"]:
                ri_result = check_referential_integrity(conn, t["table"], t["fks"])
                print(f"{t['table']}: RI={ri_result}")

            # Role C: Normalization
            norm_result = check_normalization(conn, t["table"], t["pk"], t["cols"])
            print(f"{t['table']}: normalized={norm_result}")

        # After all tables processed: flush logs into checkdb.sql
        testcase_name = Path(schema_file).stem
        flush_ri_sql_to_checkdb(testcase_name)
        flush_norm_sql_to_checkdb(testcase_name)

        conn.close()

    else:
        # Optional: simple DB probe (no side effects)
        conn = cur = None
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            print("DB connection OK")
        except Exception as e:
            print(f"DB probe failed: {e}")
        finally:
            try:
                if cur: cur.close()
            finally:
                if conn: conn.close()