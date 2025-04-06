import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, List

import pandas as pd
import streamlit as st
import tiktoken
from openai import OpenAI

from models import TargetColumn

# Initialize OpenAI client
client = OpenAI()


def get_prompt_tokens(prompt: str) -> int:
    """Gets the number of tokens that a prompt is (128k is max context window)"""
    model_name = "gpt-4o-mini"
    try:
        encoding = tiktoken.encoding_for_model(model_name)
    except KeyError:
        print(f"Warning: Model {model_name} not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")

    num_tokens = len(encoding.encode(prompt))
    return num_tokens


def identify_target_sheet(xl_file, target_columns: List[TargetColumn], table_info: str = "") -> Optional[str]:
    """
    Use OpenAI to identify which sheet in an Excel file contains the target data

    Args:
        xl_file: Excel file object
        target_columns: List of target column objects to look for
        table_info: Optional string with table information

    Returns:
        Tuple of (target_sheet, confidence, reasoning) or (None, 0, error_message)
        Note: reasoning parameter is kept for backward compatibility but will contain an empty string
    """
    # Load the Excel file
    try:
        xl = pd.ExcelFile(xl_file)
        sheet_names = xl.sheet_names
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        return None

    with st.spinner("Identifying target sheet..."):
        # For each sheet, get a sample of data to analyze
        sheet_data = {}
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(xl_file, sheet_name=sheet_name)
                # Get column names and a sample of data
                sheet_data[sheet_name] = {
                    "columns": list(df.columns),
                    "sample": df.head(2).to_dict(orient="records")
                }
            except Exception as e:
                st.warning(f"Error reading sheet {sheet_name}: {e}")

        # Create prompt for OpenAI using the column metadata
        prompt = (
            "You are tasked with identifying which sheet in an Excel file contains specific data.\n\n"
            "Here are the sheets in the file and their column names and sample data:\n\n"
        )

        for sheet_name, data in sheet_data.items():
            prompt += f"Sheet name: {sheet_name}\n"
            prompt += f"Columns: {json.dumps(data['columns'])}\n"
            prompt += f"Sample data: {json.dumps(data['sample'], indent=2)}\n\n"

        # Add detailed information about the target columns
        prompt += f"The target sheet should contain columns{table_info}. Here are the specific types of columns we're looking for:\n\n"

        for column in target_columns:
            prompt += f"- {column.name} ({column.data_type}): {column.description}\n"
            if column.examples:
                prompt += f"  Examples: {', '.join(column.examples)}\n"
            if column.historical_variations:
                prompt += f"  Known column name variations: {', '.join(column.historical_variations)}\n"
            prompt += "\n"

        prompt += (
            "INSTRUCTIONS:\n"
            "- Analyze each sheet's column names and data patterns\n"
            "- Look for columns that semantically match the target columns described above\n"
            "- Consider both the column names and the data values when making your determination\n"
            "- Identify which sheet most likely contains the target data\n\n"
            "RESPONSE FORMAT:\n"
            "Respond with ONLY a valid JSON object in the following format:\n"
            "```\n"
            "{\n"
            '  "target_sheet": "sheet_name_here"\n'
            "}\n"
            "```\n"
        )
        print(prompt)
        print(f"\nNumber of tokens: {get_prompt_tokens(prompt)}")

        # Call OpenAI to get the answer
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data analysis assistant that specializes in identifying data structures. Always respond with ONLY the requested JSON format."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )

            response_content = response.choices[0].message.content.strip()
            print(f"\nResponse: {response_content}")
            print("\n--------------------------------\n")
            result = json.loads(response_content)

            if "target_sheet" not in result:
                st.error(f"No valid 'target_sheet' found in the response. Response: {response_content}")
                return None

            target_sheet = result["target_sheet"]

            if target_sheet not in sheet_names:
                st.error(f"Identified sheet '{target_sheet}' not found in the Excel file.")
                return None

            # Return empty string for reasoning to maintain compatibility
            return target_sheet
        except Exception as e:
            st.error(f"Error calling OpenAI API: {e}")
            return None


def identify_column(df: pd.DataFrame, target_column: TargetColumn, historical_mappings: Optional[Dict[str, List[str]]] = None) -> Optional[str]:
    """
    Use OpenAI to identify which column in the dataframe corresponds to the given target column

    Args:
        df: DataFrame to analyze
        target_column: TargetColumn object containing metadata
        historical_mappings: Optional dictionary of historical mappings

    Returns:
        Column name if found, None otherwise
    """
    with st.spinner(f"Identifying column for {target_column.name}..."):
        sample_data = df.head(3).to_dict(orient="records")

        # Get list of available columns
        available_columns = list(df.columns)

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
            "2. Historical column names that have been identified as matching this column type in the past\n"
            "3. The list of available columns in the dataframe\n\n"
            "INSTRUCTIONS:\n"
            "- Analyze the column names and data patterns in the sample rows\n"
            f"- Select the most likely column that represents {target_column.name}\n"
            "- Consider both semantic similarity of column names and the data values\n"
            "- You MUST select a column name from the list of available columns\n"
            "- If none of the columns seem to match, select the closest possible match from the available columns\n\n"
            "CRITICAL: Your response MUST be one of these exact column names: " + ", ".join([f'"{col}"' for col in available_columns]) + "\n\n"
            "RESPONSE FORMAT:\n"
            "Respond with ONLY a valid JSON object in the following format:\n"
            "```\n"
            "{\n"
            f'  "{target_column.name}": "column_name_here"\n'
            "}\n"
            "```\n\n"
            "Available columns:\n"
            f"{json.dumps(available_columns)}\n\n"
            "Sample rows:\n"
            f"{json.dumps(sample_data, indent=2)}\n\n"
            "Historical column names for this type:\n"
            f"{json.dumps(all_variations)}"
        )
        print(prompt)
        print(f"\nNumber of tokens: {get_prompt_tokens(prompt)}")
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data analysis assistant that specializes in identifying column types in datasets. You must only select from the available columns provided. Always respond with ONLY the requested JSON format."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )

            response_content = response.choices[0].message.content.strip()
            print(f"\nResponse: {response_content}")
            print("\n--------------------------------\n")
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


def identify_columns_with_threads(df: pd.DataFrame, target_columns: List[TargetColumn], historical_mappings: Optional[Dict[str, List[str]]] = None, update_historical: bool = True) -> Dict[str, str]:
    """
    Use threads to identify columns in parallel for multiple target columns

    Args:
        df: DataFrame to analyze
        target_columns: List of TargetColumn objects to identify
        historical_mappings: Optional dictionary of historical mappings
        update_historical: Whether to update historical mappings with new matches

    Returns:
        Dictionary mapping target column names to identified dataframe columns
    """

    # Function to process a single column in a thread
    def identify_column_thread(column):
        guessed_column = identify_column(df, column, historical_mappings)
        return column.name, guessed_column

    # Initialize the column mappings dictionary
    column_mappings = {}

    # Use ThreadPoolExecutor to parallelize column identification
    with ThreadPoolExecutor() as executor:
        # Submit all column identification tasks
        future_to_column = {
            executor.submit(identify_column_thread, column): column
            for column in target_columns
        }

        # Process results as they complete
        for future in as_completed(future_to_column):
            try:
                column_name, guessed_column = future.result()
                if guessed_column:
                    column_mappings[column_name] = guessed_column
            except Exception as exc:
                st.warning(f"Column identification thread generated an exception: {exc}")

    # Update historical mappings if requested
    if update_historical and historical_mappings:
        for column_name, guessed_column in column_mappings.items():
            if guessed_column not in historical_mappings[column_name]:
                historical_mappings[column_name].append(guessed_column)

    return column_mappings
