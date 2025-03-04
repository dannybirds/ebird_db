"""
Database connection handling for ebird_db.
"""
import os
import logging
import psycopg
from psycopg.types.string import StrDumper
from typing import LiteralString

from db import DB_NAME

logger = logging.getLogger('ebird_db')

class NullStrDumper(StrDumper):
    """
    Custom string dumper for psycopg that converts empty or whitespace-only
    strings to NULL values in the database.
    """
    def dump(self, obj: str):
        if not obj or obj.isspace():
            return None
        return super().dump(obj)

def open_connection(autocommit: bool = False) -> psycopg.Connection:
    """
    Open a connection to the PostgreSQL database.
    
    Args:
        autocommit: Whether to enable autocommit mode
        
    Returns:
        A connection object
        
    Raises:
        psycopg.Error: If the connection fails
    """
    # Get database configuration from environment
    db_name = os.getenv("DB_NAME", DB_NAME)
    db_user = os.getenv("POSTGRES_USER")
    db_pwd = os.getenv("POSTGRES_PWD")
    
    if not db_user or not db_pwd:
        raise ValueError("Database credentials not set. Set POSTGRES_USER and POSTGRES_PWD environment variables.")
    
    # Create connection string
    conn_string = f"dbname={db_name} user={db_user} password={db_pwd}"
    
    # Log connection attempt (without password)
    logger.debug(f"Connecting to database {db_name} as user {db_user}")
    
    try:
        # Create connection
        conn = psycopg.connect(conn_string, autocommit=autocommit)
        
        # Register custom string dumper
        conn.adapters.register_dumper(str, NullStrDumper)
        
        return conn
    except psycopg.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise

def vacuum(table: LiteralString):
    """
    Run VACUUM on a table to reclaim storage and update statistics.
    
    Args:
        table: The name of the table to vacuum
    """
    try:
        with open_connection(autocommit=True) as conn:
            logger.info(f"Vacuuming table {table}")
            conn.execute(f'VACUUM {table}')
    except psycopg.Error as e:
        logger.error(f"Vacuum operation failed on table {table}: {e}")
        raise