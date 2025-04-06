import streamlit as st
from dotenv import load_dotenv

from controller import (
    initialize_session_state,
    load_historical_variations,
    select_database_table,
    process_excel_file,
    identify_sheet_and_columns,
    apply_column_mappings
)
from db_utils import save_to_database
from models import AVAILABLE_TABLES

# Load environment variables
load_dotenv()


def show_database_dependency_warning():
    """Show warning about missing database dependencies"""
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


def show_file_upload():
    """Display file upload interface and process uploaded file"""
    # File uploader
    uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])

    if uploaded_file is not None:
        # Store the file in session state for later use
        st.session_state._uploaded_file = uploaded_file

        # Show file details
        col1, col2 = st.columns(2)
        with col1:
            st.write("**File Details:**")
            file_details = {"Filename": uploaded_file.name, "File size": f"{uploaded_file.size / 1024:.2f} KB"}
            for key, value in file_details.items():
                st.write(f"**{key}:** {value}")

        st.markdown("---")

        # Process the Excel file
        excel_data = process_excel_file(uploaded_file)
        if not excel_data["success"]:
            st.error(f"Error processing file: {excel_data['error']}")
            return

        # Display the sheets in tabs
        st.subheader("All Excel Sheets")
        st.write(f"This Excel file contains {len(excel_data['sheets'])} sheets:")

        # Create tabs for each sheet
        tabs = st.tabs(excel_data["sheets"])

        # Display each sheet in its own tab
        for i, sheet_name in enumerate(excel_data["sheets"]):
            df = excel_data["dataframes"][sheet_name]

            with tabs[i]:
                st.write(f"### Sheet: {sheet_name}")
                st.write(f"Contains {df.shape[0]} rows and {df.shape[1]} columns")
                st.dataframe(df, use_container_width=True)

        st.markdown("---")

        # Process the data automatically
        with st.container():
            st.subheader("Sheet Identification and Column Mapping")

            with st.spinner("Analyzing Excel file..."):
                # Identify the target sheet and columns
                results = identify_sheet_and_columns(excel_data)

                if not results["success"]:
                    st.error(f"Analysis failed: {results.get('error', 'Unknown error')}")
                    return

                # Show sheet identification results
                target_sheet = results["target_sheet"]
                confidence = results["confidence"]

                st.success(f"Identified target sheet: **{target_sheet}** with confidence {confidence:.2f}")

                # Allow user to override the selected sheet
                st.write("**Override Target Sheet Selection:**")
                selected_sheet = st.selectbox(
                    "Select the sheet containing data:",
                    options=excel_data["sheets"],
                    index=excel_data["sheets"].index(target_sheet),
                    key="sheet_selector"
                )

                # Get the dataframe for the selected sheet
                df = excel_data["dataframes"][selected_sheet]

                # Show column mapping section
                st.subheader("Column Mapping")

                # Display the AI mappings
                ai_mappings = results["column_mappings"]

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
                        if column.name in ai_mappings:
                            try:
                                default_idx = df_columns_with_none.index(ai_mappings[column.name])
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
                            user_column_mappings[column.name] = selected_col

                # Apply the user-selected mappings
                if user_column_mappings:
                    formatted_df = apply_column_mappings(df, user_column_mappings)

                    # Store the formatted DataFrame in session state
                    st.session_state.formatted_df = formatted_df

                    # Show formatted data
                    st.subheader("Formatted Data")
                    st.dataframe(formatted_df, use_container_width=True)

                    # Show column mapping summary
                    with st.expander("View Column Mapping Summary"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**Original Column Names**")
                            for target_col, orig_col in user_column_mappings.items():
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
                                label="Download processed data as CSV",
                                data=csv,
                                file_name=f"processed_{uploaded_file.name.split('.')[0]}.csv",
                                mime="text/csv",
                            )

                        # Add button to save to database
                        with col2:
                            if st.button("Save to Database", type="primary"):
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
                            label="Download processed data as CSV",
                            data=csv,
                            file_name=f"processed_{uploaded_file.name.split('.')[0]}.csv",
                            mime="text/csv",
                            type="primary"
                        )
                else:
                    st.warning("No column mappings selected. Please select at least one column mapping.")


def main():
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

    # Update title based on mode
    if st.session_state.db_mode:
        st.title("Database Excel Processor")
    else:
        st.title("Excel Data Processor")

    # Show dependency warning if necessary
    if not st.session_state.db_mode:
        show_database_dependency_warning()

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

    # Main content area - Table selection step in DB mode
    if st.session_state.db_mode and not st.session_state.table_selected:
        show_table_selection()
    # If a table is selected or in standalone mode, proceed with file upload
    else:
        # Display the selected table in database mode
        if st.session_state.db_mode and st.session_state.selected_table:
            st.subheader(f"Processing for: {st.session_state.selected_table_schema}.{st.session_state.selected_table}")

            # Display target columns
            show_column_definitions()

            # Option to select a different table
            if st.button("Select Different Table"):
                # Reset the session state
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()  # Refresh the page to go back to table selection
        else:
            # In standalone mode, just display target columns
            show_column_definitions()

        # Show file upload interface
        show_file_upload()


if __name__ == "__main__":
    main()
