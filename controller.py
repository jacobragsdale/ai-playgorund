import json
from typing import Dict, Any

import pandas as pd
import streamlit as st

from ai_utils import identify_target_sheet, identify_columns_with_threads
from db_utils import DatabaseUtils
from models import DEFAULT_TARGET_COLUMNS


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


def load_historical_variations():
    """Load historical column name variations and update the target column objects"""
    if not hasattr(st.session_state, 'TARGET_COLUMN_DICT') or not st.session_state.selected_table:
        return

    try:
        # Get the current table identifier
        current_table = f"{st.session_state.selected_table_schema}.{st.session_state.selected_table}"
        
        # Load historical column variations from the single JSON file
        with open("historical_column_variations.json", "r") as f:
            all_variations = json.load(f)
            
            # Get variations specific to the current table
            table_variations = all_variations.get(current_table, {})
            
            # Update the target column objects with the historical variations
            for col_name, col_variations in table_variations.items():
                if col_name in st.session_state.TARGET_COLUMN_DICT:
                    st.session_state.TARGET_COLUMN_DICT[col_name].historical_variations = col_variations
    except Exception as e:
        st.warning(f"Could not load historical column variations: {e}")
        # Create a default empty mapping if the file doesn't exist
        try:
            # Create the file with an empty structure for the current table
            current_table = f"{st.session_state.selected_table_schema}.{st.session_state.selected_table}"
            all_variations = {current_table: {}}
            
            # Initialize with empty lists for each column
            for col_name in st.session_state.TARGET_COLUMN_DICT:
                all_variations[current_table][col_name] = []
                
            # Save the initial structure
            with open("historical_column_variations.json", "w") as f:
                json.dump(all_variations, f, indent=2)
        except Exception as write_error:
            st.warning(f"Could not create historical_column_variations.json: {write_error}")


def select_database_table(schema: str, table: str) -> bool:
    """
    Select a database table and load its column definitions
    
    Args:
        schema: Schema name
        table: Table name
        
    Returns:
        Success status
    """
    # Store the selection in session state
    st.session_state.selected_table = table
    st.session_state.selected_table_schema = schema

    # Load column definitions from the selected table
    db_utils = DatabaseUtils()
    target_columns = db_utils.generate_target_columns_from_db(table, schema)
    if target_columns:
        # Update session state
        st.session_state.TARGET_COLUMNS = target_columns
        st.session_state.TARGET_COLUMN_DICT = {col.name: col for col in target_columns}
        st.session_state.TARGET_COLUMN_NAMES = [col.name for col in target_columns]
        st.session_state.table_selected = True

        # Load historical variations if available
        load_historical_variations()
        return True

    # If we couldn't get column definitions for some reason, still mark table as selected
    st.session_state.table_selected = True
    return False


def process_excel_file(uploaded_file) -> Dict[str, Any]:
    """
    Process an Excel file and return structured data about it
    
    Args:
        uploaded_file: Uploaded Excel file
        
    Returns:
        Dict with file info, sheets, dataframes, etc.
    """
    result = {
        "filename": uploaded_file.name,
        "size": uploaded_file.size,
        "sheets": [],
        "dataframes": {},
        "success": False,
        "error": None
    }

    try:
        # Load all sheets from the Excel file
        xl = pd.ExcelFile(uploaded_file)
        sheet_names = xl.sheet_names
        result["sheets"] = sheet_names

        # Read each sheet into a dataframe
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                result["dataframes"][sheet_name] = df
            except Exception as e:
                st.warning(f"Error reading sheet {sheet_name}: {e}")

        result["success"] = True
        return result
    except Exception as e:
        result["error"] = str(e)
        return result


def identify_sheet_and_columns(excel_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Identify the target sheet and columns in the Excel file
    
    Args:
        excel_data: Dict with Excel file data
        
    Returns:
        Dict with identification results
    """
    result = {
        "target_sheet": None,
        "confidence": 0,
        "column_mappings": {},
        "success": False
    }

    # Skip if no Excel data
    if not excel_data["success"]:
        result["error"] = "No valid Excel data to process"
        return result

    # Get table info for the prompt
    table_info = ""
    if hasattr(st.session_state, 'selected_table') and st.session_state.selected_table:
        table_info = f" related to {st.session_state.selected_table_schema}.{st.session_state.selected_table} table data"

    # Identify the target sheet
    uploaded_file = st.session_state.get("_uploaded_file")
    if not uploaded_file:
        result["error"] = "No uploaded file in session state"
        return result

    target_sheet = identify_target_sheet(
        uploaded_file,
        st.session_state.TARGET_COLUMNS,
        table_info
    )

    if not target_sheet:
        result["error"] = "Could not identify target sheet"
        return result

    result["target_sheet"] = target_sheet

    # Get the dataframe for the target sheet
    df = excel_data["dataframes"].get(target_sheet)
    if df is None:
        result["error"] = f"Could not load data for sheet {target_sheet}"
        return result

    # Identify columns
    try:
        # Load historical mappings for the current table
        current_table = f"{st.session_state.selected_table_schema}.{st.session_state.selected_table}"
        historical_mappings = {}
        
        try:
            with open("historical_column_variations.json", "r") as f:
                all_mappings = json.load(f)
                historical_mappings = all_mappings.get(current_table, {})
        except Exception:
            # If file doesn't exist, create empty mappings
            historical_mappings = {col.name: [] for col in st.session_state.TARGET_COLUMNS}

        # Initialize mappings for columns that don't have entries yet
        for column in st.session_state.TARGET_COLUMNS:
            historical_mappings.setdefault(column.name, [])

        # Use the new utility function to identify columns with threads
        column_mappings = identify_columns_with_threads(
            df, 
            st.session_state.TARGET_COLUMNS, 
            historical_mappings,
            update_historical=True
        )

        # Save updated mappings
        try:
            # Load the entire file first to preserve mappings for other tables
            try:
                with open("historical_column_variations.json", "r") as f:
                    all_mappings = json.load(f)
            except Exception:
                all_mappings = {}
            
            # Update the mappings for the current table
            all_mappings[current_table] = historical_mappings
            
            # Save back to file
            with open("historical_column_variations.json", "w") as f:
                json.dump(all_mappings, f, indent=2)
        except Exception as e:
            st.warning(f"Could not save historical column variations: {e}")

        result["column_mappings"] = column_mappings
        result["success"] = True
        return result

    except Exception as e:
        result["error"] = str(e)
        return result


def apply_column_mappings(df: pd.DataFrame, mappings: Dict[str, str]) -> pd.DataFrame:
    """
    Apply column mappings to a dataframe
    
    Args:
        df: Input dataframe
        mappings: Dictionary of target_col -> excel_col
        
    Returns:
        DataFrame with target column names and data from mapped Excel columns
    """
    # Create a new empty dataframe with target column names
    result_df = pd.DataFrame()
    
    # For each target column, get data from the mapped Excel column
    for target_col, excel_col in mappings.items():
        if excel_col in df.columns:
            # Add the data from the Excel column to the result dataframe with the target column name
            result_df[target_col] = df[excel_col]
    
    return result_df
