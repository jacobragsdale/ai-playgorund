import os
import pyodbc
from dotenv import load_dotenv
from process_excel_data import TargetColumn

load_dotenv()

def generate_target_columns_from_db(table_name, schema="dbo", server=None, database=None, username=None, password=None, connection_string=None):
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
            raise ValueError("Database connection details incomplete. Provide either a connection string or server & database.")
        
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

# Example usage
if __name__ == "__main__":
    # You can set environment variables or provide connection details directly
    # Example: 
    # os.environ["DB_SERVER"] = "your_server"
    # os.environ["DB_NAME"] = "your_database"
    
    # Call the function with your table name
    columns = generate_target_columns_from_db("YourTableName")
    
    # Print the generated column definitions
    for col in columns:
        print(f"Column: {col.name}, Type: {col.data_type}")
        print(f"Description: {col.description}")
        if col.examples:
            print(f"Examples: {', '.join(col.examples)}")
        print("-" * 50) 