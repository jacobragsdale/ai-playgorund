import streamlit as st
from dotenv import load_dotenv

from ai_utils import identify_columns_with_threads
from controller import (
    initialize_session_state,
    load_historical_variations,
    select_database_table,
    process_excel_file,
    identify_sheet_and_columns,
    apply_column_mappings
)
from db_utils import DatabaseUtils
from models import AVAILABLE_TABLES

# Load environment variables
load_dotenv()


def show_table_selection():
    """Show table selection interface"""
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

        # Select the table and load columns
        success = select_database_table(schema, table)
        if success:
            st.success(f"Loaded column definitions from {schema}.{table}")
            st.rerun()  # Refresh the page to move to the next step
        else:
            st.error("Failed to load column definitions. Using default columns instead.")
            st.rerun()


def show_column_definitions():
    """Show target column definitions in the UI"""
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


def update_formatted_df(df, user_column_mappings):
    """Update the formatted dataframe based on user mappings"""
    if not user_column_mappings:
        return None

    # Apply the mappings to get formatted dataframe
    formatted_df = apply_column_mappings(df, user_column_mappings)

    # Store the result in session state
    st.session_state.formatted_df = formatted_df
    st.session_state.user_column_mappings = user_column_mappings

    return formatted_df


def show_file_upload():
    """Display file upload interface and process uploaded file"""
    # File uploader
    uploaded_file = st.file_uploader("Upload Deal Sheet", type=["xlsx", "xls"])

    if uploaded_file is not None:
        # Store the file in session state for later use
        st.session_state._uploaded_file = uploaded_file

        st.markdown("---")

        # Check if we need to reprocess or if we have cached results
        file_changed = "excel_data" not in st.session_state or "prev_file_name" not in st.session_state or st.session_state.prev_file_name != uploaded_file.name

        # Process the Excel file if it's new
        if file_changed:
            with st.spinner("Processing Excel file..."):
                excel_data = process_excel_file(uploaded_file)
                if not excel_data["success"]:
                    st.error(f"Error processing file: {excel_data['error']}")
                    return

                # Store in session state for reuse
                st.session_state.excel_data = excel_data
                st.session_state.prev_file_name = uploaded_file.name
        else:
            # Use cached data
            excel_data = st.session_state.excel_data

        # Display the sheets in tabs
        display_excel_sheets(excel_data)
        st.markdown("---")

        # Process the data automatically - only if we don't already have results
        process_and_display_data(excel_data, uploaded_file)


def display_excel_sheets(excel_data):
    """Display Excel sheets in tabs with the AI suggestion highlighted"""
    st.subheader("All Excel Sheets")
    st.write(f"This Excel file contains {len(excel_data['sheets'])} sheets:")

    # Get the AI-suggested sheet if available
    ai_suggested_sheet = st.session_state.get("ai_suggested_sheet")

    # Create an ordered list of sheets, with the AI-suggested sheet first
    ordered_sheets = excel_data["sheets"].copy()
    if ai_suggested_sheet in ordered_sheets:
        # Move the AI-suggested sheet to the beginning
        ordered_sheets.remove(ai_suggested_sheet)
        ordered_sheets.insert(0, f"{ai_suggested_sheet} (AI suggestion)")

    # Create tabs for each sheet
    tabs = st.tabs(ordered_sheets)

    # Display each sheet in its own tab
    for i, tab_name in enumerate(ordered_sheets):
        # Get the actual sheet name (removing the AI suggestion marker if present)
        if "(AI suggestion)" in tab_name:
            sheet_name = tab_name.split(" (AI suggestion)")[0]
        else:
            sheet_name = tab_name

        df = excel_data["dataframes"][sheet_name]

        with tabs[i]:
            if sheet_name == ai_suggested_sheet:
                st.write(f"### Sheet: {sheet_name} (AI suggestion)")
            else:
                st.write(f"### Sheet: {sheet_name}")

            st.write(f"Contains {df.shape[0]} rows and {df.shape[1]} columns")
            st.dataframe(df, use_container_width=True)


def process_and_display_data(excel_data, uploaded_file):
    """Process and display the data with AI analysis, sheet selection, and column mapping"""
    st.subheader("Override Target Sheet Selection")

    # Run AI analysis if needed
    results = run_ai_analysis(excel_data)
    if not results["success"]:
        st.error(f"Analysis failed: {results.get('error', 'Unknown error')}")
        return

    # Show sheet identification results
    target_sheet = results["target_sheet"]

    # Allow sheet override
    selected_sheet = show_sheet_override(excel_data, target_sheet)

    # Get the dataframe for the selected sheet
    df = excel_data["dataframes"][selected_sheet]
    st.session_state.selected_sheet_df = df

    # Handle sheet change if needed
    if "sheet_changed" in st.session_state and st.session_state.sheet_changed:
        handle_sheet_change(excel_data, selected_sheet, results)

    # Show column mapping options
    st.markdown("---")
    st.subheader("Override Column Mappings")

    # Display column mapping form
    show_column_mapping_form(df, results["column_mappings"])

    # Display formatted data if available
    if "formatted_df" in st.session_state and st.session_state.formatted_df is not None:
        show_formatted_data(st.session_state.formatted_df)


def run_ai_analysis(excel_data):
    """Run AI analysis on the Excel data"""
    # Check if we need to run the AI analysis
    file_changed = "prev_file_name" not in st.session_state
    if "analysis_results" not in st.session_state or file_changed:
        with st.spinner("Analyzing Excel file..."):
            # Identify the target sheet and columns
            results = identify_sheet_and_columns(excel_data)

            # Store results in session state
            st.session_state.analysis_results = results

            # Automatically apply AI-suggested mappings on initial load
            if results["success"]:
                target_sheet = results["target_sheet"]
                df = excel_data["dataframes"][target_sheet]
                st.session_state.selected_sheet_df = df

                # Use AI mappings as initial mappings
                ai_mappings = results["column_mappings"]

                # Update formatted dataframe with AI mappings
                if ai_mappings:
                    formatted_df = apply_column_mappings(df, ai_mappings)
                    st.session_state.formatted_df = formatted_df
                    st.session_state.user_column_mappings = ai_mappings

                # Store the AI-suggested sheet for highlighting in the Excel sheets section
                st.session_state.ai_suggested_sheet = target_sheet

                # Force a rerun to update the Excel sheets section
                st.rerun()
    else:
        # Use cached results
        results = st.session_state.analysis_results

    return results


def show_sheet_override(excel_data, target_sheet):
    """Display UI for overriding the target sheet selection"""

    # Define callback for sheet selection change
    def on_sheet_change():
        # Extract the original sheet name (without AI suggestion marker)
        sheet_name = st.session_state.sheet_selector
        if "(AI suggestion)" in sheet_name:
            sheet_name = sheet_name.split(" (AI suggestion)")[0]

        # Update selected sheet DataFrame
        st.session_state.selected_sheet_df = excel_data["dataframes"][sheet_name]

        # Reset column mappings
        st.session_state.user_column_mappings = {}
        st.session_state.formatted_df = None

        # Set a flag to indicate that sheet has changed and needs analysis
        st.session_state.sheet_changed = True

    # Create a version of sheet names with AI suggestion marked
    marked_sheets = excel_data["sheets"].copy()
    target_sheet_index = excel_data["sheets"].index(target_sheet)
    marked_sheets[target_sheet_index] = f"{target_sheet} (AI suggestion)"

    selected_sheet = st.selectbox(
        "If the target sheet was not correctly identified, update it here. Column mappings will be rerun with the newly select sheet",
        options=marked_sheets,
        index=target_sheet_index,
        key="sheet_selector",
        on_change=on_sheet_change
    )

    # Extract the original sheet name (without AI suggestion marker)
    if "(AI suggestion)" in selected_sheet:
        selected_sheet = selected_sheet.split(" (AI suggestion)")[0]

    return selected_sheet


def handle_sheet_change(excel_data, selected_sheet, results):
    """Handle sheet change by recalculating column mappings"""
    with st.spinner(f"Analyzing sheet '{selected_sheet}'..."):
        # Get the dataframe for the new sheet
        new_df = excel_data["dataframes"][selected_sheet]

        # Use the shared utility function to identify columns with threads
        # For sheet change, we don't need to update historical mappings
        new_mappings = identify_columns_with_threads(
            new_df,
            st.session_state.TARGET_COLUMNS,
            update_historical=False
        )

        # Store the new mappings in the session state
        if new_mappings:
            # Update the results with the new mappings
            results["column_mappings"] = new_mappings
            # Update the UI with the new mappings
            st.session_state.user_column_mappings = new_mappings

            # Apply the new mappings to get a formatted DataFrame
            formatted_df = apply_column_mappings(new_df, new_mappings)
            st.session_state.formatted_df = formatted_df

            # Instead of manipulating session state directly, we'll let the
            # selectbox widgets handle their own state based on the updated ai_mappings

    # Reset the flag
    st.session_state.sheet_changed = False


def show_column_mapping_form(df, ai_mappings):
    """Display form for column mapping overrides"""
    with st.form(key="column_mapping_form"):
        st.write("If any of the column mappings are incorrect, update them here.")

        # Add "None" option for columns that don't exist in the dataset
        df_columns_with_none = ["None"] + list(df.columns)

        # Create user-editable column mappings
        user_column_mappings = {}
        cols = st.columns(3)
        for i, column in enumerate(st.session_state.TARGET_COLUMNS):
            col_idx = i % 3
            with cols[col_idx]:
                # Create a version of the dropdown options with stars for AI-suggested mappings
                marked_columns = df_columns_with_none.copy()

                # Get current value from session state if available
                key = f"col_map_{column.name}"

                # Set default to the AI suggestion if available
                default_idx = 0
                ai_suggestion = None

                if column.name in ai_mappings:
                    try:
                        ai_suggestion = ai_mappings[column.name]
                        default_idx = df_columns_with_none.index(ai_suggestion)

                        # Mark the AI suggestion with a star in the dropdown
                        for j, col_name in enumerate(marked_columns):
                            if col_name == ai_suggestion:
                                marked_columns[j] = f"{col_name} (AI suggestion)"
                    except ValueError:
                        default_idx = 0

                # Get current value from session state if available
                if key in st.session_state and isinstance(st.session_state[key], str):
                    try:
                        # Find the index in original list (without stars or AI suggestion text)
                        orig_value = st.session_state[key].replace("* ", "").split(" (AI suggestion)")[0]
                        if orig_value in df_columns_with_none:
                            current_idx = df_columns_with_none.index(orig_value)
                            default_idx = current_idx
                    except (ValueError, IndexError):
                        pass

                # Display the dropdown with marked options
                selected_col = st.selectbox(
                    f"{column.name} ({column.data_type}):",
                    options=marked_columns,
                    index=default_idx,
                    help=column.description,
                    key=key
                )

                # Convert selected value back to original column name
                if selected_col != "None":
                    # Extract the original column name without the star/AI suggestion text
                    orig_col = selected_col.replace("* ", "").split(" (AI suggestion)")[0]
                    user_column_mappings[column.name] = orig_col

        # Submit button for the form
        submit_button = st.form_submit_button("Override Mappings")

        if submit_button:
            # Update formatted_df in session state
            st.session_state.user_column_mappings = user_column_mappings
            formatted_df = update_formatted_df(df, user_column_mappings)
            st.session_state.formatted_df = formatted_df


def show_formatted_data(formatted_df):
    """Display the formatted data with row deletion functionality"""
    st.markdown("---")
    st.subheader("Formatted Data")

    # Add row deletion functionality
    if "rows_to_delete" not in st.session_state:
        st.session_state.rows_to_delete = set()

    # Create a container for the data and deletion controls
    deletion_status = st.empty()  # Placeholder for dynamic status message

    # Display the current selection count (will update dynamically)
    if len(st.session_state.rows_to_delete) > 0:
        deletion_status.warning(f"Selected {len(st.session_state.rows_to_delete)} rows for deletion")
    else:
        deletion_status.empty()  # Don't show any message when no rows are selected

    # Show delete button and handle deletion
    show_delete_button(formatted_df, deletion_status)

    # Display the dataframe with checkboxes for selecting rows to delete
    show_data_editor(formatted_df, deletion_status)

    # Show download and save options
    show_download_save_options(formatted_df)


def show_delete_button(formatted_df, deletion_status):
    """Display delete button and handle row deletion"""
    if st.button("Delete Selected Rows", disabled=len(st.session_state.rows_to_delete) == 0, key="delete_button"):
        # Store the count of rows to be deleted for the success message
        num_rows_deleted = len(st.session_state.rows_to_delete)

        # Filter out the selected rows
        formatted_df = formatted_df.drop(index=list(st.session_state.rows_to_delete))

        # Update session state
        st.session_state.formatted_df = formatted_df

        # Clear selections
        st.session_state.rows_to_delete = set()

        # Show success message
        st.success(f"Deleted {num_rows_deleted} rows")

        # Clear the selection message
        deletion_status.empty()


def show_data_editor(formatted_df, deletion_status):
    """Display the data editor with row selection checkboxes"""
    data_container = st.container()
    with data_container:
        # Add a checkbox column to the DataFrame
        display_df = formatted_df.copy()
        display_df["_select_"] = False

        # Get list of original columns (excluding _select_)
        data_columns = formatted_df.columns.tolist()

        # Rearrange columns to put _select_ first
        select_cols = ["_select_"] + data_columns
        display_df = display_df[select_cols]

        # Use the Streamlit data editor with checkbox column
        edited_df = st.data_editor(
            display_df,
            column_config={
                "_select_": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select rows to delete",
                    default=False,
                    width="small"  # Make the select column narrower
                ),
            },
            disabled=data_columns,  # Only disable data columns, not the checkbox column
            hide_index=True,
            use_container_width=True,
            key="data_editor"
        )

        # Update rows_to_delete based on checked boxes
        # Reset the selection set
        st.session_state.rows_to_delete = set()

        # Add the checked rows to the set
        if "_select_" in edited_df.columns:
            for idx, row in edited_df.iterrows():
                if row["_select_"]:
                    st.session_state.rows_to_delete.add(idx)

        # Update the selection message dynamically
        if len(st.session_state.rows_to_delete) > 0:
            deletion_status.warning(f"Selected {len(st.session_state.rows_to_delete)} rows for deletion")
        else:
            deletion_status.empty()  # Don't show any message when no rows are selected


def show_download_save_options(formatted_df):
    """Show download and save options for the formatted data"""
    st.subheader("Download or Save Data")
    st.write(f"Save the formatted data as displayed to {st.session_state.selected_table_schema}.{st.session_state.selected_table}")

    if st.button(f"Write to DB table {st.session_state.selected_table_schema}.{st.session_state.selected_table}", type="primary"):
        db_utils = DatabaseUtils()
        success, message = db_utils.save_to_database(
            formatted_df,
            st.session_state.selected_table,
            st.session_state.selected_table_schema
        )

        if success:
            st.success(message)
        else:
            st.error(message)


def show_sidebar():
    """Display the sidebar with app information and instructions"""
    with st.sidebar:
        st.header("About")
        st.info("""
        This app processes Excel files and maps them to database tables:
        - Select a target database table
        - Upload an Excel file with relevant data
        - The app maps columns from Excel to database columns
        - Save the processed data back to the database
        """)

        st.header("Instructions")
        st.markdown("""
        1. Select the target database table
        2. Upload an Excel file using the file uploader
        3. Review all sheets in the uploaded file
        4. The app will automatically identify the sheet with relevant data
        5. You can override the selected sheet and column mappings if needed
        6. Save the processed data to the database
        """)


def main():
    """Main function to run the Streamlit app"""
    # Set page title and configuration
    st.set_page_config(
        page_title="Excel Data Processor",
        page_icon=None,
        layout="wide"
    )

    # Initialize session state
    initialize_session_state()

    # Load historical column variations
    load_historical_variations()

    # Set title
    st.title("Database Excel Processor")

    # Sidebar for app navigation and information
    show_sidebar()

    # Main content area - Table selection step
    if not st.session_state.table_selected:
        show_table_selection()
    # If a table is selected, proceed with file upload
    else:
        # Display the selected table
        st.subheader(f"Processing for: {st.session_state.selected_table_schema}.{st.session_state.selected_table}")

        # Display target columns
        show_column_definitions()

        # Option to select a different table
        if st.button("Select Different Table"):
            # Reset the session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()  # Refresh the page to go back to table selection

        # Show file upload interface
        show_file_upload()


if __name__ == "__main__":
    main()
