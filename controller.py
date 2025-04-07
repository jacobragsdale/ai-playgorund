import json
from typing import Dict, Any, List

import pandas as pd
import streamlit as st

from ai_utils import identify_target_sheet, identify_columns
from db_utils import DatabaseUtils
from models import DEFAULT_TARGET_COLUMNS


# === SESSION STATE MANAGEMENT ===

def initialize_session_state():
    """Initialize core session state variables"""
    # Table selection state
    if 'table_selected' not in st.session_state:
        st.session_state.table_selected = False
    if 'selected_table' not in st.session_state:
        st.session_state.selected_table = None
    if 'selected_table_schema' not in st.session_state:
        st.session_state.selected_table_schema = None

    # Target columns state
    if 'TARGET_COLUMNS' not in st.session_state:
        st.session_state.TARGET_COLUMNS = DEFAULT_TARGET_COLUMNS
    if 'TARGET_COLUMN_DICT' not in st.session_state:
        st.session_state.TARGET_COLUMN_DICT = {col.name: col for col in DEFAULT_TARGET_COLUMNS}
    if 'TARGET_COLUMN_NAMES' not in st.session_state:
        st.session_state.TARGET_COLUMN_NAMES = [col.name for col in DEFAULT_TARGET_COLUMNS]

    # Data state
    if 'formatted_df' not in st.session_state:
        st.session_state.formatted_df = None
    if 'rows_to_delete' not in st.session_state:
        st.session_state.rows_to_delete = set()


# === DATABASE OPERATIONS ===

def select_database_table(schema: str, table: str) -> bool:
    """
    Load database table column definitions and update session state

    Args:
        schema: Database schema name
        table: Database table name

    Returns:
        bool: Success status
    """
    # Store the selection in session state
    st.session_state.selected_table = table
    st.session_state.selected_table_schema = schema

    # Load column definitions from the selected table
    db_utils = DatabaseUtils()
    target_columns = db_utils.generate_target_columns_from_db(table, schema)

    if target_columns:
        # Update session state with table column information
        st.session_state.TARGET_COLUMNS = target_columns
        st.session_state.TARGET_COLUMN_DICT = {col.name: col for col in target_columns}
        st.session_state.TARGET_COLUMN_NAMES = [col.name for col in target_columns]
        st.session_state.table_selected = True

        # Load historical mappings for this table
        load_historical_variations()
        return True

    # If we couldn't get column definitions, still mark table as selected
    st.session_state.table_selected = True
    return False


def save_to_database(formatted_df: pd.DataFrame) -> tuple[bool, str]:
    """Save formatted dataframe to the selected database table"""
    db_utils = DatabaseUtils()
    return db_utils.save_to_database(
        formatted_df,
        st.session_state.selected_table,
        st.session_state.selected_table_schema
    )


# === HISTORICAL MAPPING MANAGEMENT ===

def load_historical_variations() -> Dict[str, List[str]]:
    """Load historical column name variations for the selected table"""
    if not hasattr(st.session_state, 'TARGET_COLUMN_DICT') or not st.session_state.selected_table:
        return {}

    try:
        # Get the current table identifier
        current_table = f"{st.session_state.selected_table_schema}.{st.session_state.selected_table}"

        try:
            with open("historical_column_variations.json", "r") as f:
                all_variations = json.load(f)
                historical_mappings = all_variations.get(current_table, {})

                # Update the target column objects with the historical variations
                for col_name, col_variations in historical_mappings.items():
                    if col_name in st.session_state.TARGET_COLUMN_DICT:
                        st.session_state.TARGET_COLUMN_DICT[col_name].historical_variations = col_variations
                return historical_mappings
        except FileNotFoundError:
            return create_empty_historical_mappings()

    except Exception:
        return {}


def create_empty_historical_mappings() -> Dict[str, List[str]]:
    """Create empty historical mappings file for the current table"""
    try:
        current_table = f"{st.session_state.selected_table_schema}.{st.session_state.selected_table}"
        all_variations = {current_table: {}}

        # Initialize with empty lists for each column
        for col_name in st.session_state.TARGET_COLUMN_DICT:
            all_variations[current_table][col_name] = []

        # Save the initial structure
        with open("historical_column_variations.json", "w") as f:
            json.dump(all_variations, f, indent=2)

        return all_variations[current_table]
    except Exception:
        return {}


def save_historical_variations(historical_mappings: Dict[str, List[str]]):
    """Save updated historical variations to JSON file"""
    try:
        current_table = f"{st.session_state.selected_table_schema}.{st.session_state.selected_table}"

        try:
            with open("historical_column_variations.json", "r") as f:
                all_mappings = json.load(f)
        except Exception:
            all_mappings = {}

        all_mappings[current_table] = historical_mappings

        with open("historical_column_variations.json", "w") as f:
            json.dump(all_mappings, f, indent=2)
    except Exception:
        pass


# === EXCEL PROCESSING ===

def process_excel_file(uploaded_file) -> Dict[str, Any]:
    """Read and process an Excel file"""
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
            except Exception:
                pass

        result["success"] = True
        return result
    except Exception as e:
        result["error"] = str(e)
        return result


def identify_sheet_and_columns(excel_data: Dict[str, Any]) -> Dict[str, Any]:
    """Identify the target sheet and columns in the Excel file"""
    result = {
        "target_sheet": None,
        "column_mappings": {},
        "success": False,
        "error": None
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

    target_sheet = identify_target_sheet(uploaded_file, st.session_state.TARGET_COLUMNS, table_info)

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
        historical_mappings = load_historical_variations()

        column_mappings = identify_columns(df, st.session_state.TARGET_COLUMNS, historical_mappings, update_historical=True)

        # Save updated mappings
        save_historical_variations(historical_mappings)

        result["column_mappings"] = column_mappings
        result["success"] = True
        return result

    except Exception as e:
        result["error"] = str(e)
        return result


def analyze_new_sheet(excel_data: Dict[str, Any], selected_sheet: str) -> Dict[str, str]:
    """Analyze a new sheet when user overrides the AI suggestion"""
    new_df = excel_data["dataframes"][selected_sheet]
    return identify_columns(new_df, st.session_state.TARGET_COLUMNS, update_historical=False)


def apply_column_mappings(df: pd.DataFrame, mappings: Dict[str, str]) -> pd.DataFrame:
    """Apply column mappings to create properly formatted dataframe"""
    result_df = pd.DataFrame()

    # Apply the mappings in the order defined in TARGET_COLUMNS
    for target_col_obj in st.session_state.TARGET_COLUMNS:
        target_col = target_col_obj.name
        
        # Skip if this target column isn't in the mappings
        if target_col not in mappings:
            continue
            
        excel_col = mappings[target_col]
        if excel_col in df.columns:
            result_df[target_col] = df[excel_col]

    return result_df


def delete_selected_rows(formatted_df: pd.DataFrame, rows_to_delete: set) -> pd.DataFrame:
    """Delete selected rows from the formatted dataframe"""
    if not rows_to_delete:
        return formatted_df
        
    # Filter out the selected rows
    return formatted_df.drop(index=list(rows_to_delete))
