import streamlit as st
from dotenv import load_dotenv
import pandas as pd

from controller import (
    initialize_session_state,
    load_historical_variations,
    select_database_table,
    process_excel_file,
    identify_sheet_and_columns,
    apply_column_mappings,
    analyze_new_sheet,
    delete_selected_rows,
    save_to_database
)
from models import AVAILABLE_TABLES

# Load environment variables
load_dotenv()


def show_table_and_column_selection():
    """Display the table selection interface and column definitions"""
    # Table selection UI
    st.subheader("Select Target Database Table")
    st.write("Choose the database table that will define column mappings and where data will be saved.")

    table_options = [f"{t['schema']}.{t['name']}" for t in AVAILABLE_TABLES]
    selected_table_full = st.selectbox(
        "Select Database Table:",
        options=table_options,
        index=0 if table_options else None,
        help="Select the database table that contains your target schema"
    )

    if selected_table_full and st.button("Continue with Selected Table", type="primary"):
        schema, table = selected_table_full.split('.')
        success = select_database_table(schema, table)
        if success:
            st.success(f"Loaded column definitions from {schema}.{table}")
            st.rerun()
        else:
            st.error("Failed to load column definitions. Using default columns instead.")
            st.rerun()
    
    # If we already have a table selected, show column definitions
    if st.session_state.table_selected:
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


def process_excel_upload():
    """Handle file upload, sheet display, and data processing"""
    uploaded_file = st.file_uploader("Upload Deal Sheet", type=["xlsx", "xls"])

    if uploaded_file is None:
        return

    # Process the uploaded file
    st.session_state._uploaded_file = uploaded_file
    st.markdown("---")

    # Check if we need to reprocess or if we have cached results
    file_changed = (
        "excel_data" not in st.session_state or
        "prev_file_name" not in st.session_state or
        st.session_state.prev_file_name != uploaded_file.name
    )

    # Process the Excel file if it's new
    if file_changed:
        with st.spinner("Processing Excel file..."):
            excel_data = process_excel_file(uploaded_file)
            if not excel_data["success"]:
                st.error(f"Error processing file: {excel_data['error']}")
                return

            st.session_state.excel_data = excel_data
            st.session_state.prev_file_name = uploaded_file.name
    else:
        excel_data = st.session_state.excel_data

    # Display all the Excel sheets with tabs
    display_excel_sheets(excel_data)
    st.markdown("---")
    
    # Process and analyze the data
    analyze_and_map_data(excel_data)


def display_excel_sheets(excel_data):
    """Display Excel sheets in tabs with the AI suggestion highlighted"""
    st.subheader("All Excel Sheets")
    st.write(f"This Excel file contains {len(excel_data['sheets'])} sheets:")

    ai_suggested_sheet = st.session_state.get("ai_suggested_sheet")

    ordered_sheets = excel_data["sheets"].copy()
    if ai_suggested_sheet in ordered_sheets:
        ordered_sheets.remove(ai_suggested_sheet)
        ordered_sheets.insert(0, f"{ai_suggested_sheet} (AI suggestion)")

    tabs = st.tabs(ordered_sheets)

    for i, tab_name in enumerate(ordered_sheets):
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


def analyze_and_map_data(excel_data):
    """Process and display data with AI analysis, sheet selection, and column mapping"""
    st.subheader("Override Target Sheet Selection")

    # Run AI analysis 
    file_changed = "prev_file_name" not in st.session_state
    if "analysis_results" not in st.session_state or file_changed:
        with st.spinner("Analyzing Excel file..."):
            results = identify_sheet_and_columns(excel_data)
            st.session_state.analysis_results = results

            if results["success"]:
                target_sheet = results["target_sheet"]
                df = excel_data["dataframes"][target_sheet]
                st.session_state.selected_sheet_df = df

                ai_mappings = results["column_mappings"]
                if ai_mappings:
                    formatted_df = apply_column_mappings(df, ai_mappings)
                    st.session_state.formatted_df = formatted_df
                    st.session_state.user_column_mappings = ai_mappings

                st.session_state.ai_suggested_sheet = target_sheet
                st.rerun()
    else:
        results = st.session_state.analysis_results

    if not results["success"]:
        st.error(f"Analysis failed: {results.get('error', 'Unknown error')}")
        return

    target_sheet = results["target_sheet"]

    # Allow sheet override with callback
    def on_sheet_change():
        sheet_name = st.session_state.sheet_selector
        if "(AI suggestion)" in sheet_name:
            sheet_name = sheet_name.split(" (AI suggestion)")[0]

        st.session_state.selected_sheet_df = excel_data["dataframes"][sheet_name]
        st.session_state.user_column_mappings = {}
        st.session_state.formatted_df = None
        st.session_state.sheet_changed = True

    marked_sheets = excel_data["sheets"].copy()
    target_sheet_index = excel_data["sheets"].index(target_sheet)
    marked_sheets[target_sheet_index] = f"{target_sheet} (AI suggestion)"

    selected_sheet = st.selectbox(
        "Override the sheet selection if needed. Column mappings will be reanalyzed.",
        options=marked_sheets,
        index=target_sheet_index,
        key="sheet_selector",
        on_change=on_sheet_change
    )

    if "(AI suggestion)" in selected_sheet:
        selected_sheet = selected_sheet.split(" (AI suggestion)")[0]

    # Get dataframe for selected sheet
    df = excel_data["dataframes"][selected_sheet]
    st.session_state.selected_sheet_df = df

    # Handle sheet change if needed
    if "sheet_changed" in st.session_state and st.session_state.sheet_changed:
        with st.spinner(f"Analyzing sheet '{selected_sheet}'..."):
            new_mappings = analyze_new_sheet(excel_data, selected_sheet)

            if new_mappings:
                results["column_mappings"] = new_mappings
                st.session_state.user_column_mappings = new_mappings

                formatted_df = apply_column_mappings(df, new_mappings)
                st.session_state.formatted_df = formatted_df

        st.session_state.sheet_changed = False

    # Display column mapping options
    st.markdown("---")
    st.subheader("Override Column Mappings")
    display_column_mapping_options(df, results["column_mappings"])

    # Display formatted data if available
    if "formatted_df" in st.session_state and st.session_state.formatted_df is not None:
        display_formatted_data(st.session_state.formatted_df)


def display_column_mapping_options(df, ai_mappings):
    """Display column mapping options that update automatically when changed"""
    st.write("If any of the column mappings are incorrect, update them here.")

    df_columns_with_none = ["None"] + list(df.columns)

    # Function to update formatted df when selections change
    def on_column_mapping_change():
        user_column_mappings = {}
        for column in st.session_state.TARGET_COLUMNS:
            key = f"col_map_{column.name}"
            if key in st.session_state and st.session_state[key] != "None":
                orig_col = st.session_state[key].replace("* ", "").split(" (AI suggestion)")[0]
                user_column_mappings[column.name] = orig_col
        
        # Update formatted dataframe
        if user_column_mappings:
            formatted_df = apply_column_mappings(df, user_column_mappings)
            st.session_state.formatted_df = formatted_df
            st.session_state.user_column_mappings = user_column_mappings

    # Create column selection UI
    cols = st.columns(3)
    for i, column in enumerate(st.session_state.TARGET_COLUMNS):
        col_idx = i % 3
        with cols[col_idx]:
            marked_columns = df_columns_with_none.copy()
            default_idx = 0

            if column.name in ai_mappings:
                try:
                    ai_suggestion = ai_mappings[column.name]
                    default_idx = df_columns_with_none.index(ai_suggestion)

                    for j, col_name in enumerate(marked_columns):
                        if col_name == ai_suggestion:
                            marked_columns[j] = f"{col_name} (AI suggestion)"
                except ValueError:
                    default_idx = 0

            key = f"col_map_{column.name}"
            if key in st.session_state and isinstance(st.session_state[key], str):
                try:
                    orig_value = st.session_state[key].replace("* ", "").split(" (AI suggestion)")[0]
                    if orig_value in df_columns_with_none:
                        current_idx = df_columns_with_none.index(orig_value)
                        default_idx = current_idx
                except (ValueError, IndexError):
                    pass

            st.selectbox(
                f"{column.name} ({column.data_type}):",
                options=marked_columns,
                index=default_idx,
                help=column.description,
                key=key,
                on_change=on_column_mapping_change
            )


def display_formatted_data(formatted_df):
    """Display the formatted data with row deletion and save functionality"""
    st.markdown("---")
    st.subheader("Formatted Data")

    deletion_status = st.empty()
    if len(st.session_state.rows_to_delete) > 0:
        deletion_status.warning(f"Selected {len(st.session_state.rows_to_delete)} rows for deletion")
    else:
        deletion_status.empty()

    # Delete button functionality
    if st.button("Delete Selected Rows", disabled=len(st.session_state.rows_to_delete) == 0, key="delete_button"):
        num_rows_deleted = len(st.session_state.rows_to_delete)
        updated_df = delete_selected_rows(formatted_df, st.session_state.rows_to_delete)
        st.session_state.formatted_df = updated_df
        st.session_state.rows_to_delete = set()
        st.success(f"Deleted {num_rows_deleted} rows")
        deletion_status.empty()

    # Data editor with row selection
    with st.container():
        display_df = formatted_df.copy()
        display_df["_select_"] = False

        data_columns = [col.name for col in st.session_state.TARGET_COLUMNS if col.name in formatted_df.columns]
        select_cols = ["_select_"] + data_columns
        display_df = display_df[select_cols]

        edited_df = st.data_editor(
            display_df,
            column_config={
                "_select_": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select rows to delete",
                    default=False,
                    width="small"
                ),
            },
            disabled=data_columns,
            hide_index=True,
            use_container_width=True,
            key="data_editor"
        )

        st.session_state.rows_to_delete = set()
        if "_select_" in edited_df.columns:
            for idx, row in edited_df.iterrows():
                if row["_select_"]:
                    st.session_state.rows_to_delete.add(idx)

        if len(st.session_state.rows_to_delete) > 0:
            deletion_status.warning(f"Selected {len(st.session_state.rows_to_delete)} rows for deletion")
        else:
            deletion_status.empty()

    # Save functionality
    st.subheader("Write to Database")
    st.write(f"Save the formatted data as displayed to {st.session_state.selected_table_schema}.{st.session_state.selected_table}")

    if st.button(f"Write to DB table {st.session_state.selected_table_schema}.{st.session_state.selected_table}", type="primary"):
        success, message = save_to_database(formatted_df)
        if success:
            st.success(message)
        else:
            st.error(message)


def show_sidebar():
    """Display the sidebar with app information and instructions"""
    with st.sidebar:
        st.header("About")
        st.info("""
        This app helps you map Excel data to database tables:
        
        - Automatically identifies the right sheet and columns in your Excel file
        - Maps Excel columns to your database schema
        - Allows you to override any AI-suggested mappings
        - Lets you remove unwanted rows before saving
        - Preserves column mappings to improve future accuracy
        """)

        st.header("Instructions")
        st.markdown("""
        ### 1. Select Target Table
        - Choose the database table that defines your target schema
        - This determines which columns the app will look for
        
        ### 2. Upload Excel File
        - Upload an Excel file containing your data
        - The app works with .xlsx and .xls formats
        
        ### 3. Review & Adjust
        - The app automatically identifies the most relevant sheet
        - Review all sheets in the Excel file in the tabs
        - AI-suggested sheets and columns are marked
        - Override the sheet selection if needed
        
        ### 4. Column Mapping
        - The app maps Excel columns to database columns
        - Modify any mapping by selecting from the dropdown
        - Changes apply automatically when you select a new value
        
        ### 5. Remove Unwanted Rows
        - Check the boxes next to rows you want to remove
        - Click "Delete Selected Rows" to remove them
        
        ### 6. Save to Database
        - Review the final formatted data
        - Click "Write to DB table" to save the data
        """)
        
        st.markdown("---")
        
        st.header("Tips")
        st.markdown("""
        - Column mappings are learned over time
        - The select column is for row deletion only
        - All columns display in database schema order
        - You can start over by clicking "Select Different Table"
        """)


def main():
    """Main function to run the Streamlit app"""
    st.set_page_config(
        page_title="Excel Data Processor",
        page_icon=None,
        layout="wide"
    )

    initialize_session_state()
    load_historical_variations()
    st.title("Database Excel Processor")
    show_sidebar()

    if not st.session_state.table_selected:
        show_table_and_column_selection()
    else:
        st.subheader(f"Processing for: {st.session_state.selected_table_schema}.{st.session_state.selected_table}")
        show_table_and_column_selection()

        if st.button("Select Different Table"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

        process_excel_upload()


if __name__ == "__main__":
    main()
