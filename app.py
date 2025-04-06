import streamlit as st
import pandas as pd
import json
import io
import os
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize OpenAI client
client = OpenAI()

# Try to import pyodbc, but make it optional
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

# Import generate_column_definitions conditionally
try:
    from generate_column_definitions import generate_target_columns_from_db
    COLUMN_GENERATOR_AVAILABLE = True
except ImportError:
    COLUMN_GENERATOR_AVAILABLE = False

@dataclass
class TargetColumn:
    """Class for defining target columns and their properties"""
    name: str  # Standard column name
    data_type: str  # Data type (string, number, date, etc.)
    description: str  # Description of what this column represents
    examples: List[str] = field(default_factory=list)  # Example values for this column
    historical_variations: List[str] = field(default_factory=list)  # Known variations of column names
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

# Fallback target columns in case we can't load from database
DEFAULT_TARGET_COLUMNS = [
    TargetColumn(
        name="account_id",
        data_type="string",
        description="Unique identifier for the account",
        examples=["AC12345", "10042", "ACCT-987654321"]
    ),
    TargetColumn(
        name="balance",
        data_type="number",
        description="Current account balance in currency units",
        examples=["1250.00", "$5,423.50", "10000.75"]
    ),
    TargetColumn(
        name="open_date",
        data_type="date",
        description="Date when the account was opened",
        examples=["2020-01-15", "2019-06-30", "2022-12-01"]
    ),
    TargetColumn(
        name="status",
        data_type="string",
        description="Current status of the account",
        examples=["active", "inactive", "pending"]
    ),
    TargetColumn(
        name="customer_name",
        data_type="string",
        description="Full name of the customer or account holder",
        examples=["John Doe", "Jane Smith", "Acme Corporation"]
    )
]

# List of available database tables
AVAILABLE_TABLES = [
    {"schema": "dbo", "name": "Accounts"},
    {"schema": "dbo", "name": "Customers"},
    {"schema": "sales", "name": "Transactions"}
    # Add more tables as needed
]

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

def save_to_database(df, table_name, schema="dbo"):
    """Save the DataFrame to the specified database table"""
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

def load_historical_variations():
    """Load historical column name variations and update the target column objects"""
    if not hasattr(st.session_state, 'TARGET_COLUMN_DICT'):
        return
    
    try:
        # Try to load historical column variations from column_variations.json
        with open("column_variations.json", "r") as f:
            variations = json.load(f)
            
            # Update the target column objects with the historical variations
            for col_name, col_variations in variations.items():
                if col_name in st.session_state.TARGET_COLUMN_DICT:
                    st.session_state.TARGET_COLUMN_DICT[col_name].historical_variations = col_variations
    except Exception as e:
        st.warning(f"Could not load column variations: {e}")
    
    try:
        # Also try to load from account_mapping.json if it exists
        with open("account_mapping.json", "r") as f:
            mappings = json.load(f)
            
            # Update with any additional variations from account mappings
            for col_name, col_variations in mappings.items():
                if col_name in st.session_state.TARGET_COLUMN_DICT:
                    current_variations = set(st.session_state.TARGET_COLUMN_DICT[col_name].historical_variations)
                    # Add any new variations not already in the list
                    for var in col_variations:
                        if var not in current_variations:
                            st.session_state.TARGET_COLUMN_DICT[col_name].historical_variations.append(var)
    except Exception as e:
        st.warning(f"Could not load account mappings: {e}")

def identify_target_sheet(xl_file):
    """
    Use OpenAI to identify which sheet in an Excel file contains the target data
    """
    # Load the Excel file
    try:
        xl = pd.ExcelFile(xl_file)
        sheet_names = xl.sheet_names
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        return None, 0, str(e)
    
    with st.spinner("Identifying target sheet..."):
        # For each sheet, get a sample of data to analyze
        sheet_data = {}
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(xl_file, sheet_name=sheet_name)
                # Get column names and a sample of data
                sheet_data[sheet_name] = {
                    "columns": list(df.columns),
                    "sample": df.head(3).to_dict(orient="records")
                }
            except Exception as e:
                st.warning(f"Error reading sheet {sheet_name}: {e}")
        
        # Create prompt for OpenAI using the enhanced column metadata
        prompt = (
            "You are tasked with identifying which sheet in an Excel file contains specific data.\n\n"
            "Here are the sheets in the file and their column names and sample data:\n\n"
        )
        
        for sheet_name, data in sheet_data.items():
            prompt += f"Sheet name: {sheet_name}\n"
            prompt += f"Columns: {json.dumps(data['columns'])}\n"
            prompt += f"Sample data: {json.dumps(data['sample'], indent=2)}\n\n"
        
        # Add detailed information about the target columns
        table_info = ""
        if hasattr(st.session_state, 'selected_table') and st.session_state.selected_table:
            table_info = f" related to {st.session_state.selected_table_schema}.{st.session_state.selected_table} table data"
        
        prompt += f"The target sheet should contain columns{table_info}. Here are the specific types of columns we're looking for:\n\n"
        
        for column in st.session_state.TARGET_COLUMNS:
            prompt += f"- {column.name} ({column.data_type}): {column.description}\n"
            if column.examples:
                prompt += f"  Examples: {', '.join(column.examples)}\n"
            if column.historical_variations:
                prompt += f"  Known column name variations: {', '.join(column.historical_variations)}\n"
            prompt += "\n"
        
        prompt += (
            "INSTRUCTIONS:\n"
            "- Analyze each sheet's column names and data patterns\n"
            "- Look for columns that semantically match the target database columns described above\n"
            "- Consider both the column names and the data values when making your determination\n"
            "- Identify which sheet most likely contains the target data\n\n"
            "RESPONSE FORMAT:\n"
            "Respond with ONLY a valid JSON object in the following format:\n"
            "```\n"
            "{\n"
            '  "target_sheet": "sheet_name_here",\n'
            '  "confidence": 0.95,\n'
            '  "reasoning": "Brief explanation of why this sheet was selected"\n'
            "}\n"
            "```\n"
            "The confidence score should be between 0 and 1, where 1 is absolute certainty.\n"
        )

        # Call OpenAI to get the answer
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a data analysis assistant that specializes in identifying database-like data structures. Always respond with ONLY the requested JSON format."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )

            response_content = response.choices[0].message.content.strip()
            result = json.loads(response_content)
            
            if "target_sheet" not in result:
                st.error(f"No valid 'target_sheet' found in the response. Response: {response_content}")
                return None, 0, "Failed to identify target sheet"
            
            target_sheet = result["target_sheet"]
            confidence = result.get("confidence", 0)
            reasoning = result.get("reasoning", "No reasoning provided")
            
            if target_sheet not in sheet_names:
                st.error(f"Identified sheet '{target_sheet}' not found in the Excel file.")
                return None, 0, f"Identified sheet '{target_sheet}' not found in the Excel file"
            
            return target_sheet, confidence, reasoning
        except Exception as e:
            st.error(f"Error calling OpenAI API: {e}")
            return None, 0, str(e)

def identify_column(df, target_column, historical_mappings=None):
    """
    Use OpenAI to identify which column in the dataframe corresponds to the given target column
    
    Args:
        df: DataFrame to analyze
        target_column: TargetColumn object containing metadata
        historical_mappings: Optional dictionary of historical mappings
    """
    with st.spinner(f"Identifying column for {target_column.name}..."):
        sample_data = df.head(3).to_dict(orient="records")
        
        # Combine historical variations from both sources
        all_variations = target_column.historical_variations.copy()
        if historical_mappings and target_column.name in historical_mappings:
            for var in historical_mappings[target_column.name]:
                if var not in all_variations:
                    all_variations.append(var)

        prompt = (
            f"You are tasked with identifying the column that represents '{target_column.name}' in a dataset.\n\n"
            f"Column description: {target_column.description}\n"
            f"Expected data type: {target_column.data_type}\n"
            f"Example values: {', '.join(target_column.examples)}\n\n"
            "Given the following information:\n"
            "1. Sample data rows (first rows of the dataframe along with column names)\n"
            "2. Historical column names that have been identified as matching this column type in the past\n\n"
            "INSTRUCTIONS:\n"
            "- Analyze the column names and data patterns in the sample rows\n"
            f"- Select the most likely column that represents {target_column.name}\n"
            "- Consider both semantic similarity of column names and the data values\n\n"
            "RESPONSE FORMAT:\n"
            "Respond with ONLY a valid JSON object in the following format:\n"
            "```\n"
            "{\n"
            f'  "{target_column.name}": "column_name_here"\n'
            "}\n"
            "```\n\n"
            "Sample rows:\n"
            f"{json.dumps(sample_data, indent=2)}\n\n"
            "Historical column names for this type:\n"
            f"{json.dumps(all_variations)}"
        )

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data analysis assistant that specializes in identifying column types in datasets. Always respond with ONLY the requested JSON format."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )

            response_content = response.choices[0].message.content.strip()
            guessed_column = json.loads(response_content).get(target_column.name)

            if not guessed_column:
                st.error(f"No valid '{target_column.name}' column found in the response. Response: {response_content}")
                return None

            if guessed_column not in df.columns:
                st.error(f"Guessed column '{guessed_column}' was not found in the dataframe columns.")
                return None

            return guessed_column
        except Exception as e:
            st.error(f"Error identifying column {target_column.name}: {e}")
            return None

def format_dataframe_columns(df, historical_mappings=None):
    """
    Identify and rename columns in a dataframe to standardized column names
    """
    if historical_mappings is None:
        # Load historical mappings if available
        try:
            with open("account_mapping.json", "r") as f:
                historical_mappings = json.load(f)
        except Exception:
            historical_mappings = {col.name: [] for col in st.session_state.TARGET_COLUMNS}

    # Initialize mappings for columns that don't have entries yet
    for column in st.session_state.TARGET_COLUMNS:
        historical_mappings.setdefault(column.name, [])
    
    column_mappings = {}
    
    # Process each target column
    for column in st.session_state.TARGET_COLUMNS:
        try:
            guessed_column = identify_column(df, column, historical_mappings)
            
            if guessed_column:
                # Update historical mappings
                if guessed_column not in historical_mappings[column.name]:
                    historical_mappings[column.name].append(guessed_column)
                
                column_mappings[guessed_column] = column.name
                st.info(f"Mapping '{guessed_column}' to '{column.name}'")
        except Exception as e:
            st.error(f"Error identifying {column.name}: {e}")
    
    # Save updated mappings
    try:
        with open("account_mapping.json", "w") as f:
            json.dump(historical_mappings, f, indent=2)
    except Exception as e:
        st.warning(f"Could not save mappings: {e}")
    
    # Rename the columns
    if column_mappings:
        df = df.rename(columns=column_mappings)
    
    return df

def initialize_session_state():
    """Initialize session state variables"""
    if 'table_selected' not in st.session_state:
        st.session_state.table_selected = False
    if 'selected_table' not in st.session_state:
        st.session_state.selected_table = None
    if 'selected_table_schema' not in st.session_state:
        st.session_state.selected_table_schema = None
    if 'TARGET_COLUMNS' not in st.session_state:
        st.session_state.TARGET_COLUMNS = DEFAULT_TARGET_COLUMNS
    if 'TARGET_COLUMN_DICT' not in st.session_state:
        st.session_state.TARGET_COLUMN_DICT = {col.name: col for col in DEFAULT_TARGET_COLUMNS}
    if 'TARGET_COLUMN_NAMES' not in st.session_state:
        st.session_state.TARGET_COLUMN_NAMES = [col.name for col in DEFAULT_TARGET_COLUMNS]
    if 'formatted_df' not in st.session_state:
        st.session_state.formatted_df = None
    if 'db_mode' not in st.session_state:
        st.session_state.db_mode = False

def load_table_columns(schema, table_name):
    """Load column definitions from the selected database table"""
    if not PYODBC_AVAILABLE or not COLUMN_GENERATOR_AVAILABLE:
        st.error("Database functionality is not available. Missing required dependencies.")
        return None
    
    try:
        # Generate target columns from database table
        target_columns = generate_target_columns_from_db(table_name, schema)
        
        # Update session state
        st.session_state.TARGET_COLUMNS = target_columns
        st.session_state.TARGET_COLUMN_DICT = {col.name: col for col in target_columns}
        st.session_state.TARGET_COLUMN_NAMES = [col.name for col in target_columns]
        st.session_state.table_selected = True
        
        # Load historical variations if available
        load_historical_variations()
        
        return f"Loaded {len(target_columns)} columns from {schema}.{table_name}"
    except Exception as e:
        st.error(f"Error loading table columns: {e}")
        return None

def main():
    # Set page title and configuration
    st.set_page_config(
        page_title="Excel Data Processor",
        page_icon="📊",
        layout="wide"
    )
    
    # Initialize session state
    initialize_session_state()
    
    # Update title based on mode
    if st.session_state.db_mode:
        st.title("Database Excel Processor 📊")
    else:
        st.title("Excel Data Processor 📊")
    
    # Show dependency warning if necessary
    if not PYODBC_AVAILABLE:
        st.warning("""
        ### Database functionality is disabled
        
        The app is running in standalone mode without database connectivity.
        
        **Missing dependency: pyodbc**
        
        To enable database features, install the required dependencies:
        
        ```bash
        # macOS
        brew install unixodbc
        pip install pyodbc
        
        # Windows
        pip install pyodbc
        # Ensure you have SQL Server ODBC drivers installed
        
        # Linux
        sudo apt-get install unixodbc-dev
        pip install pyodbc
        ```
        """)
    
    # Sidebar for app navigation and information
    with st.sidebar:
        st.header("About")
        if st.session_state.db_mode:
            st.info("""
            This app processes Excel files and maps them to database tables:
            - Select a target database table
            - Upload an Excel file with relevant data
            - The app maps columns from Excel to database columns
            - Save the processed data back to the database
            """)
        else:
            st.info("""
            This app processes Excel files:
            - Upload an Excel file with relevant data
            - The app identifies sheets and maps columns
            - Download the processed data as CSV
            """)
        
        st.header("Instructions")
        if st.session_state.db_mode:
            st.markdown("""
            1. Select the target database table
            2. Upload an Excel file using the file uploader
            3. Review all sheets in the uploaded file
            4. The app will automatically identify the sheet with relevant data
            5. You can override the selected sheet and column mappings if needed
            6. Save the processed data to the database
            """)
        else:
            st.markdown("""
            1. Upload an Excel file using the file uploader
            2. Review all sheets in the uploaded file
            3. The app will automatically identify the sheet with relevant data
            4. You can override the selected sheet and column mappings if needed
            5. Download the processed data as CSV
            """)
    
    # Main content area - Table selection step
    if st.session_state.db_mode and not st.session_state.table_selected:
        st.subheader("Step 1: Select Target Database Table")
        st.write("Choose the database table that will define column mappings and where data will be saved.")
        
        # Create a list of table names for the dropdown
        table_options = [f"{t['schema']}.{t['name']}" for t in AVAILABLE_TABLES]
        
        # Table selection dropdown
        selected_table_full = st.selectbox(
            "Select Database Table:", 
            options=table_options,
            index=0 if table_options else None,
            help="Select the database table that contains your target schema"
        )
        
        if selected_table_full and st.button("Continue with Selected Table", type="primary"):
            # Parse schema and table name
            schema, table = selected_table_full.split('.')
            
            # Store the selection in session state
            st.session_state.selected_table = table
            st.session_state.selected_table_schema = schema
            
            # Load column definitions from the selected table
            result = load_table_columns(schema, table)
            if result:
                st.success(result)
                st.rerun()  # Refresh the page to move to the next step
    
    # If a table is selected or in standalone mode, proceed with file upload
    else:
        # Display the selected table in database mode
        if st.session_state.db_mode and st.session_state.selected_table:
            st.subheader(f"Processing for: {st.session_state.selected_table_schema}.{st.session_state.selected_table}")
            
            # Display target columns in an expander
            with st.expander("View Target Column Definitions"):
                col1, col2, col3 = st.columns(3)
                columns = [col1, col2, col3]
                
                for i, col in enumerate(st.session_state.TARGET_COLUMNS):
                    with columns[i % 3]:
                        st.markdown(f"**{col.name}** ({col.data_type})")
                        st.write(f"Description: {col.description}")
                        if col.examples:
                            st.write(f"Examples: {', '.join(col.examples)}")
                        st.write("---")
            
            # Option to select a different table
            if st.button("⬅️ Select Different Table"):
                # Reset the session state
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()  # Refresh the page to go back to table selection
        else:
            # In standalone mode, display target columns
            with st.expander("View Target Column Definitions"):
                col1, col2, col3 = st.columns(3)
                columns = [col1, col2, col3]
                
                for i, col in enumerate(st.session_state.TARGET_COLUMNS):
                    with columns[i % 3]:
                        st.markdown(f"**{col.name}** ({col.data_type})")
                        st.write(f"Description: {col.description}")
                        if col.examples:
                            st.write(f"Examples: {', '.join(col.examples)}")
                        st.write("---")
        
        # File uploader
        uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])
        
        if uploaded_file is not None:
            # Show file details
            col1, col2 = st.columns(2)
            with col1:
                st.write("**File Details:**")
                file_details = {"Filename": uploaded_file.name, "File size": f"{uploaded_file.size / 1024:.2f} KB"}
                for key, value in file_details.items():
                    st.write(f"**{key}:** {value}")
            
            st.markdown("---")
            
            # Display all sheets in the Excel file and collect data
            try:
                # Load all sheets from the Excel file
                xl = pd.ExcelFile(uploaded_file)
                sheet_names = xl.sheet_names
                
                with st.container():
                    st.subheader("📑 All Excel Sheets")
                    st.write(f"This Excel file contains {len(sheet_names)} sheets:")
                    
                    # Create tabs for each sheet
                    tabs = st.tabs(sheet_names)
                    all_dfs = {}
                    
                    # Display each sheet in its own tab
                    for i, sheet_name in enumerate(sheet_names):
                        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                        all_dfs[sheet_name] = df
                        
                        with tabs[i]:
                            st.write(f"### Sheet: {sheet_name}")
                            st.write(f"Contains {df.shape[0]} rows and {df.shape[1]} columns")
                            st.dataframe(df, use_container_width=True)
                
                st.markdown("---")
                
                # Process the uploaded file automatically
                with st.container():
                    # Step 1: Identify which sheet contains the target data
                    st.subheader("🔍 Sheet Identification and Column Mapping")
                    
                    with st.spinner("Analyzing Excel file..."):
                        # Identify the target sheet automatically
                        target_sheet, confidence, reasoning = identify_target_sheet(uploaded_file)
                        
                        if target_sheet:
                            # Create a success message with the identified sheet
                            st.success(f"AI identified target sheet: **{target_sheet}** with confidence {confidence:.2f}")
                            st.write(f"**Reasoning:** {reasoning}")
                            
                            # Allow user to override the selected sheet
                            st.write("**Override Target Sheet Selection:**")
                            selected_sheet = st.selectbox(
                                "Select the sheet containing data:",
                                options=sheet_names,
                                index=sheet_names.index(target_sheet),
                                key="sheet_selector"
                            )
                            
                            # Load the selected sheet (either AI-identified or user-selected)
                            df = all_dfs[selected_sheet]
                            
                            # Initialize column mapping process
                            st.subheader("📋 Column Mapping")
                            
                            # Perform automatic column mapping
                            with st.spinner("Mapping columns automatically..."):
                                # First get the automatic mappings
                                historical_mappings = None
                                try:
                                    with open("account_mapping.json", "r") as f:
                                        historical_mappings = json.load(f)
                                except Exception:
                                    historical_mappings = {col.name: [] for col in st.session_state.TARGET_COLUMNS}
                                
                                # Initialize mappings for columns that don't have entries yet
                                for column in st.session_state.TARGET_COLUMNS:
                                    historical_mappings.setdefault(column.name, [])
                                
                                # Process each target column to get AI suggestions
                                auto_column_mappings = {}
                                for column in st.session_state.TARGET_COLUMNS:
                                    try:
                                        guessed_column = identify_column(df, column, historical_mappings)
                                        
                                        if guessed_column:
                                            # Update historical mappings
                                            if guessed_column not in historical_mappings[column.name]:
                                                historical_mappings[column.name].append(guessed_column)
                                            
                                            auto_column_mappings[column.name] = guessed_column
                                            st.info(f"AI mapped '{guessed_column}' to '{column.name}'")
                                    except Exception as e:
                                        st.warning(f"Could not identify mapping for {column.name}: {e}")
                            
                            # Allow user to override column mappings
                            st.write("**Override Column Mappings:**")
                            st.write("Select the appropriate column from your data for each target column:")
                            
                            # Add "None" option for columns that don't exist in the dataset
                            df_columns_with_none = ["None"] + list(df.columns)
                            
                            # Create user-editable column mappings
                            user_column_mappings = {}
                            cols = st.columns(3)
                            for i, column in enumerate(st.session_state.TARGET_COLUMNS):
                                col_idx = i % 3
                                with cols[col_idx]:
                                    # Set default to the AI suggestion if available
                                    default_idx = 0
                                    if column.name in auto_column_mappings:
                                        try:
                                            default_idx = df_columns_with_none.index(auto_column_mappings[column.name])
                                        except ValueError:
                                            default_idx = 0
                                    
                                    selected_col = st.selectbox(
                                        f"{column.name} ({column.data_type}):",
                                        options=df_columns_with_none,
                                        index=default_idx,
                                        help=column.description,
                                        key=f"col_map_{column.name}"
                                    )
                                    
                                    if selected_col != "None":
                                        user_column_mappings[selected_col] = column.name
                            
                            # Save updated mappings
                            try:
                                with open("account_mapping.json", "w") as f:
                                    json.dump(historical_mappings, f, indent=2)
                            except Exception as e:
                                st.warning(f"Could not save mappings: {e}")
                            
                            # Apply the user-selected mappings
                            if user_column_mappings:
                                formatted_df = df.rename(columns=user_column_mappings)
                                
                                # Only keep columns that were mapped to a target column
                                target_cols = set(st.session_state.TARGET_COLUMN_NAMES).intersection(formatted_df.columns)
                                if target_cols:
                                    formatted_df = formatted_df[list(target_cols)]
                                
                                # Store the formatted DataFrame in session state
                                st.session_state.formatted_df = formatted_df
                                
                                # Show formatted data
                                st.subheader("✅ Formatted Data")
                                st.dataframe(formatted_df, use_container_width=True)
                                
                                # Show column mapping summary
                                with st.expander("View Column Mapping Summary"):
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.write("**Original Column Names**")
                                        for orig_col, target_col in user_column_mappings.items():
                                            st.write(f"- {orig_col} → {target_col}")
                                    with col2:
                                        st.write("**Standardized Column Names**")
                                        for col in formatted_df.columns:
                                            st.write(f"- {col}")
                                
                                # Allow downloading the processed data
                                csv = formatted_df.to_csv(index=False)
                                
                                if st.session_state.db_mode:
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.download_button(
                                            label="⬇️ Download processed data as CSV",
                                            data=csv,
                                            file_name=f"processed_{uploaded_file.name.split('.')[0]}.csv",
                                            mime="text/csv",
                                        )
                                    
                                    # Add button to save to database
                                    with col2:
                                        if st.button("💾 Save to Database", type="primary"):
                                            success, message = save_to_database(
                                                formatted_df, 
                                                st.session_state.selected_table, 
                                                st.session_state.selected_table_schema
                                            )
                                            
                                            if success:
                                                st.success(message)
                                            else:
                                                st.error(message)
                                else:
                                    # Just download button in standalone mode
                                    st.download_button(
                                        label="⬇️ Download processed data as CSV",
                                        data=csv,
                                        file_name=f"processed_{uploaded_file.name.split('.')[0]}.csv",
                                        mime="text/csv",
                                        type="primary"
                                    )
                            else:
                                st.warning("No column mappings selected. Please select at least one column mapping.")
                        else:
                            st.error("Failed to identify a target sheet in the Excel file.")
                            
                            # Allow manual selection if AI failed
                            selected_sheet = st.selectbox(
                                "Select the sheet containing data manually:",
                                options=sheet_names,
                                key="manual_sheet_selector"
                            )
                            
                            st.warning("AI column mapping unavailable. Please manually select column mappings below.")
                            # Add manual mapping interface here similar to above
                
            except Exception as e:
                st.error(f"Error processing file: {e}")

if __name__ == "__main__":
    main() 