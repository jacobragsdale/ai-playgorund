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


# Function to update formatted dataframe when mapping changes
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


def on_mapping_change():
    """Callback when column mappings change"""
    # Update the column mappings dictionary
    user_column_mappings = {}
    for column in st.session_state.TARGET_COLUMNS:
        key = f"col_map_{column.name}"
        if key in st.session_state and st.session_state[key] != "None":
            user_column_mappings[column.name] = st.session_state[key]
    
    # Get the selected sheet's dataframe from session state
    if "selected_sheet_df" in st.session_state and st.session_state.selected_sheet_df is not None:
        df = st.session_state.selected_sheet_df
        
        # Update the formatted dataframe
        update_formatted_df(df, user_column_mappings)


def show_file_upload():
    """Display file upload interface and process uploaded file"""
    # File uploader
    uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])

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

        # Process the data automatically - only if we don't already have results
        with st.container():
            st.subheader("Sheet Identification and Column Mapping")
            
            # Check if we need to run the AI analysis
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
            else:
                # Use cached results
                results = st.session_state.analysis_results

            if not results["success"]:
                st.error(f"Analysis failed: {results.get('error', 'Unknown error')}")
                return

            # Show sheet identification results
            target_sheet = results["target_sheet"]

            st.success(f"Identified target sheet: **{target_sheet}**")

            # Allow user to override the selected sheet
            st.write("**Override Target Sheet Selection:**")
            
            # Define callback for sheet selection change
            def on_sheet_change():
                sheet_name = st.session_state.sheet_selector
                st.session_state.selected_sheet_df = excel_data["dataframes"][sheet_name]
                # Reset column mappings when sheet changes
                st.session_state.user_column_mappings = {}
                st.session_state.formatted_df = None
            
            selected_sheet = st.selectbox(
                "Select the sheet containing target data:",
                options=excel_data["sheets"],
                index=excel_data["sheets"].index(target_sheet),
                key="sheet_selector",
                on_change=on_sheet_change
            )

            # Get the dataframe for the selected sheet
            df = excel_data["dataframes"][selected_sheet]
            st.session_state.selected_sheet_df = df

            # Show column mapping section
            st.subheader("Column Mapping")

            # Display the AI mappings
            ai_mappings = results["column_mappings"]

            # Create a form for column mappings to batch the updates
            with st.form(key="column_mapping_form"):
                st.write("**Override Column Mappings:**")
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
                                        marked_columns[j] = f"* {col_name} (AI suggestion)"
                            except ValueError:
                                default_idx = 0

                        # Get current value from session state if available
                        current_value = None
                        key = f"col_map_{column.name}"
                        if key in st.session_state:
                            try:
                                # Find the index in original list (without stars)
                                orig_value = st.session_state[key].replace("* ", "").split(" (AI suggestion)")[0]
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

            # Display the formatted data outside the form to avoid rerunning when it's submitted
            if "formatted_df" in st.session_state and st.session_state.formatted_df is not None:
                formatted_df = st.session_state.formatted_df
                user_column_mappings = st.session_state.user_column_mappings
                
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
                        if st.button(f"Save to {st.session_state.selected_table_schema}.{st.session_state.selected_table}", type="primary"):
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
            elif user_column_mappings:
                st.info("Click 'Override Mappings' to update the formatted data")
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
