import os
from typing import Tuple

import pandas as pd
import pyodbc
import streamlit as st
from dotenv import load_dotenv

from models import TargetColumn

# Load environment variables
load_dotenv()


def get_db_connection():
    """Get a connection to the database"""
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
    connection = get_db_connection()
    if not connection:
        return False, "Could not establish database connection"

    cursor = connection.cursor()

    try:
        # Get column information to ensure we're mapping correctly
        cursor.execute(
            f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}' ORDER BY ORDINAL_POSITION")
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


def load_table_columns(schema: str, table_name: str):
    """
    Load column definitions from the selected database table
    
    Args:
        schema: Database schema name
        table_name: Database table name
        
    Returns:
        List of TargetColumn objects or None if not available
    """
    try:
        # Generate target columns from database table
        target_columns = generate_target_columns_from_db(table_name, schema)
        return target_columns
    except Exception as e:
        st.error(f"Error loading table columns: {e}")
        return None


def generate_target_columns_from_db(table_name, schema="dbo", server=None, database=None, username=None, password=None,
                                    connection_string=None):
    """
    Connect to a database, extract column information, and generate TargetColumn objects

    Args:
        table_name: Name of the database table
        schema: Database schema (default: dbo)
        server, database, username, password: Database connection details
        connection_string: Optional full connection string

    Returns:
        List of TargetColumn objects
    """
    # 1. Set up connection string
    if connection_string:
        conn_str = connection_string
    else:
        # Get connection details from parameters or environment variables
        server = server or os.getenv("DB_SERVER")
        database = database or os.getenv("DB_NAME")
        username = username or os.getenv("DB_USERNAME")
        password = password or os.getenv("DB_PASSWORD")

        if not (server and database):
            raise ValueError(
                "Database connection details incomplete. Provide either a connection string or server & database.")

        # Create connection string
        if username and password:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        else:
            # Use trusted connection (Windows authentication)
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"

    # 2. Connect to the database and get column information
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()

    try:
        # Query to get column metadata
        metadata_query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            ep.value as COLUMN_DESCRIPTION
        FROM 
            INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN 
            sys.extended_properties ep ON 
            ep.major_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) AND
            ep.minor_id = c.ORDINAL_POSITION AND
            ep.name = 'MS_Description'
        WHERE 
            c.TABLE_NAME = ? AND
            c.TABLE_SCHEMA = ?
        ORDER BY 
            c.ORDINAL_POSITION
        """

        cursor.execute(metadata_query, (table_name, schema))
        column_info_list = []

        # Process column metadata
        for row in cursor.fetchall():
            # Format the full data type
            data_type = row.DATA_TYPE
            if row.CHARACTER_MAXIMUM_LENGTH and row.CHARACTER_MAXIMUM_LENGTH != -1:
                data_type = f"{data_type}({row.CHARACTER_MAXIMUM_LENGTH})"
            elif row.NUMERIC_PRECISION is not None and row.NUMERIC_SCALE is not None:
                data_type = f"{data_type}({row.NUMERIC_PRECISION},{row.NUMERIC_SCALE})"

            column_info = {
                "name": row.COLUMN_NAME,
                "data_type": data_type,
                "description": row.COLUMN_DESCRIPTION or f"Column {row.COLUMN_NAME} with type {data_type}",
                "examples": []
            }
            column_info_list.append(column_info)

        # Get sample data if any columns were found
        if column_info_list:
            try:
                # Get top 3 rows of data for examples
                cursor.execute(f"SELECT TOP 3 * FROM [{schema}].[{table_name}]")
                rows = cursor.fetchall()

                if rows:
                    # Add sample values to each column
                    for i, column_info in enumerate(column_info_list):
                        samples = [str(row[i]) if row[i] is not None else "NULL" for row in rows]
                        column_info["examples"] = [s for s in samples if s != "NULL"]
            except Exception as e:
                print(f"Warning: Could not retrieve sample data: {e}")

        # 3. Create TargetColumn objects
        target_columns = []
        for info in column_info_list:
            target_columns.append(TargetColumn(
                name=info["name"].lower(),
                data_type=info["data_type"],
                description=info["description"],
                examples=info["examples"]
            ))

        print(f"Generated {len(target_columns)} column definitions for {schema}.{table_name}")
        return target_columns

    finally:
        connection.close()
