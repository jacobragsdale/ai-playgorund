import os
import pandas as pd
import streamlit as st
from typing import Tuple, Optional, List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Flag for database availability
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False


def is_db_available() -> bool:
    """Check if database functionality is available"""
    return PYODBC_AVAILABLE


def get_db_connection():
    """Get a connection to the database"""
    if not PYODBC_AVAILABLE:
        st.error("pyodbc is not available. Database connection not possible.")
        return None
    
    try:
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_NAME")
        username = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        
        if username and password:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        else:
            # Use trusted connection (Windows authentication)
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"
        
        return pyodbc.connect(conn_str)
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        return None


def save_to_database(df: pd.DataFrame, table_name: str, schema: str = "dbo") -> Tuple[bool, str]:
    """
    Save the DataFrame to the specified database table
    
    Args:
        df: DataFrame to save
        table_name: Database table name
        schema: Database schema
        
    Returns:
        Tuple of (success, message)
    """
    if not PYODBC_AVAILABLE:
        return False, "Database functionality is not available. Missing required dependency: pyodbc"
    
    connection = get_db_connection()
    if not connection:
        return False, "Could not establish database connection"
    
    cursor = connection.cursor()
    
    try:
        # Get column information to ensure we're mapping correctly
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}' ORDER BY ORDINAL_POSITION")
        db_columns = cursor.fetchall()
        db_column_names = [col.COLUMN_NAME for col in db_columns]
        
        # Ensure DataFrame columns match database columns (case-insensitive)
        df_cols = df.columns.tolist()
        matched_cols = []
        for db_col in db_column_names:
            for df_col in df_cols:
                if df_col.lower() == db_col.lower():
                    matched_cols.append((df_col, db_col))
                    break
        
        if not matched_cols:
            return False, "No matching columns found between processed data and database table"
        
        # Clear existing data (optional - could be made configurable)
        cursor.execute(f"DELETE FROM [{schema}].[{table_name}]")
        
        # Insert data
        for _, row in df.iterrows():
            # Generate placeholders and column names
            columns = [db_col for _, db_col in matched_cols]
            placeholders = ["?" for _ in matched_cols]
            
            # Get values in the correct order
            values = [row[df_col] for df_col, _ in matched_cols]
            
            # Build and execute SQL
            sql = f"INSERT INTO [{schema}].[{table_name}] ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            cursor.execute(sql, values)
        
        connection.commit()
        return True, f"Successfully saved {len(df)} records to {schema}.{table_name}"
    
    except Exception as e:
        connection.rollback()
        return False, f"Error saving to database: {str(e)}"
    
    finally:
        connection.close()


# Conditional import for generate_column_definitions
try:
    from generate_column_definitions import generate_target_columns_from_db
    COLUMN_GENERATOR_AVAILABLE = True
except ImportError:
    COLUMN_GENERATOR_AVAILABLE = False


def load_table_columns(schema: str, table_name: str):
    """
    Load column definitions from the selected database table
    
    Args:
        schema: Database schema name
        table_name: Database table name
        
    Returns:
        List of TargetColumn objects or None if not available
    """
    if not PYODBC_AVAILABLE or not COLUMN_GENERATOR_AVAILABLE:
        st.error("Database functionality is not available. Missing required dependencies.")
        return None
    
    try:
        # Generate target columns from database table
        target_columns = generate_target_columns_from_db(table_name, schema)
        return target_columns
    except Exception as e:
        st.error(f"Error loading table columns: {e}")
        return None 