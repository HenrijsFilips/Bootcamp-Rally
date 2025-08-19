# snowflake connection stuff.
# connect to snowflake and run queries. handles transactions too.

import os
from typing import Any, Dict, List, Optional, Tuple, Union

import snowflake.connector as sf
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection

# load environment
load_dotenv()

# transaction flag
IN_TRANSACTION = False


def _require_env(name: str) -> str:
    # get env var or complain if missing.
    # checks if env var exists.
    # returns the value or raises error.
    val = os.getenv(name)
    if not val:
        raise ValueError(f"Missing required environment variable: {name}")
    return val


def get_connection() -> SnowflakeConnection:
    # connect to snowflake.
    # reads settings from env vars.
    # returns active connection ready to use.
    conn = sf.connect(
        user=_require_env("SNOWFLAKE_USER"),
        password=_require_env("SNOWFLAKE_PASSWORD"),
        account=_require_env("SNOWFLAKE_ACCOUNT"),
        warehouse=_require_env("SNOWFLAKE_WAREHOUSE"),
        role=_require_env("SNOWFLAKE_ROLE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "BOOTCAMP_RALLY"),
        autocommit=False,  # be explicit; we manage commits below
        client_session_keep_alive=True,  # helpful for Streamlit
    )
    return conn


def _rows_to_dicts(cursor, rows) -> List[Dict[str, Any]]:
    # turn database rows into dicts.
    # makes data easier to work with.
    # returns list of dicts with column names as keys.
    col_names = [info[0] for info in cursor.description]
    result = []
    for row in rows:
        item = {}
        for i in range(len(col_names)):
            item[col_names[i]] = row[i]
        result.append(item)
    return result


def fetch_all(
    conn: SnowflakeConnection, sql: str, params: Optional[Tuple] = None
) -> List[Dict[str, Any]]:
    # run a select query and get all results.
    # need connection sql and optional params.
    # returns all rows as list of dicts.
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return _rows_to_dicts(cur, rows)


def fetch_one(
    conn: SnowflakeConnection, sql: str, params: Optional[Tuple] = None
) -> Optional[Dict[str, Any]]:
    # run a select query and get just first row.
    # need connection sql and maybe params.
    # returns one row as dict or none if empty.
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        if row:
            return _rows_to_dicts(cur, [row])[0]
        return None


def fetch_one_value(
    conn: SnowflakeConnection, sql: str, params: Optional[Tuple] = None
) -> Any:
    # get single value from query.
    # returns just first column of first row.
    # returns the value or none if no data.
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        if row:
            return row[0]
        return None


def execute(
    conn: SnowflakeConnection, sql: str, params: Optional[Tuple] = None
) -> int:
    # run a non-select statement like insert update delete.
    # auto commits unless in transaction.
    # returns how many rows changed.
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        if not IN_TRANSACTION:
            conn.commit()
        return cur.rowcount


def execute_many(
    conn: SnowflakeConnection, sql: str, many_params: List[Tuple]
) -> int:
    # run same sql many times with different data.
    # auto commits unless in transaction.
    # returns total rows affected.
    with conn.cursor() as cur:
        cur.executemany(sql, many_params or [])
        if not IN_TRANSACTION:
            conn.commit()
        return cur.rowcount


class Transaction:
    # transaction helper.
    # makes sure operations succeed or fail together.
    # use with "with" to wrap multiple statements.

    def __init__(self, conn: SnowflakeConnection):
        # start a transaction.
        # need a connection.
        self.conn = conn
        self._prev = False

    def __enter__(self) -> "Transaction":
        # start transaction mode.
        # turns on transaction flag.
        global IN_TRANSACTION
        self._prev = IN_TRANSACTION
        IN_TRANSACTION = True
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # end transaction.
        # commit if all good or rollback if errors.
        # returns false to let exceptions through.
        global IN_TRANSACTION
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            IN_TRANSACTION = self._prev
        # Do not suppress exceptions
        return False


def transaction(conn: SnowflakeConnection) -> Transaction:
    # make a transaction object.
    # need a connection.
    # returns transaction for with statement.
    return Transaction(conn)