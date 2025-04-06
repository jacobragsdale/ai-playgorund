import concurrent.futures
import json
import os
import glob
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any

load_dotenv()

client = OpenAI()

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

# Define all target columns with their metadata
TARGET_COLUMNS = [
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
        name="email",
        data_type="string",
        description="Contact email address for the account holder",
        examples=["user@example.com", "john.doe@company.org"]
    ),
    TargetColumn(
        name="phone",
        data_type="string",
        description="Contact phone number for the account holder",
        examples=["(123) 456-7890", "+1-555-123-4567"]
    ),
    TargetColumn(
        name="address",
        data_type="string",
        description="Physical address of the account holder",
        examples=["123 Main St, Anytown, CA 12345", "456 Oak Ave, Suite 100, Portland, OR 97123"]
    ),
    TargetColumn(
        name="customer_name",
        data_type="string",
        description="Full name of the customer or account holder",
        examples=["John Doe", "Jane Smith", "Acme Corporation"]
    ),
    TargetColumn(
        name="last_activity",
        data_type="date",
        description="Date of the most recent account activity",
        examples=["2023-01-10", "2022-11-27", "2023-03-15"]
    )
]

# Create a dictionary mapping column names to their full definitions
TARGET_COLUMN_DICT = {col.name: col for col in TARGET_COLUMNS}
TARGET_COLUMN_NAMES = [col.name for col in TARGET_COLUMNS]

def load_historical_variations():
    """Load historical column name variations and update the target column objects"""
    try:
        # Try to load historical column variations from column_variations.json
        with open("column_variations.json", "r") as f:
            variations = json.load(f)
            
            # Update the target column objects with the historical variations
            for col_name, col_variations in variations.items():
                if col_name in TARGET_COLUMN_DICT:
                    TARGET_COLUMN_DICT[col_name].historical_variations = col_variations
    except Exception as e:
        print(f"Warning: Could not load column variations: {e}")
    
    try:
        # Also try to load from account_mapping.json if it exists
        with open("account_mapping.json", "r") as f:
            mappings = json.load(f)
            
            # Update with any additional variations from account mappings
            for col_name, col_variations in mappings.items():
                if col_name in TARGET_COLUMN_DICT:
                    current_variations = set(TARGET_COLUMN_DICT[col_name].historical_variations)
                    # Add any new variations not already in the list
                    for var in col_variations:
                        if var not in current_variations:
                            TARGET_COLUMN_DICT[col_name].historical_variations.append(var)
    except Exception as e:
        print(f"Note: Could not load account mappings: {e}")

def identify_target_sheet(excel_file):
    """
    Use OpenAI to identify which sheet in an Excel file contains the target data
    """
    print(f"\nIdentifying target sheet in {excel_file}...")
    
    # Load the Excel file
    xl = pd.ExcelFile(excel_file)
    sheet_names = xl.sheet_names
    
    # For each sheet, get a sample of data to analyze
    sheet_data = {}
    for sheet_name in sheet_names:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            # Get column names and a sample of data
            sheet_data[sheet_name] = {
                "columns": list(df.columns),
                "sample": df.head(3).to_dict(orient="records")
            }
        except Exception as e:
            print(f"  Error reading sheet {sheet_name}: {e}")
    
    # Create prompt for OpenAI using the enhanced column metadata
    prompt = (
        "You are tasked with identifying which sheet in an Excel file contains specific banking account data.\n\n"
        "Here are the sheets in the file and their column names and sample data:\n\n"
    )
    
    for sheet_name, data in sheet_data.items():
        prompt += f"Sheet name: {sheet_name}\n"
        prompt += f"Columns: {json.dumps(data['columns'])}\n"
        prompt += f"Sample data: {json.dumps(data['sample'], indent=2)}\n\n"
    
    # Add detailed information about the target columns
    prompt += "The target sheet should contain columns related to banking account information. Here are the specific types of columns we're looking for:\n\n"
    
    for column in TARGET_COLUMNS:
        prompt += f"- {column.name} ({column.data_type}): {column.description}\n"
        if column.examples:
            prompt += f"  Examples: {', '.join(column.examples)}\n"
        if column.historical_variations:
            prompt += f"  Known column name variations: {', '.join(column.historical_variations)}\n"
        prompt += "\n"
    
    prompt += (
        "INSTRUCTIONS:\n"
        "- Analyze each sheet's column names and data patterns\n"
        "- Look for columns that semantically match the target banking data concepts described above\n"
        "- Consider both the column names and the data values when making your determination\n"
        "- Identify which sheet most likely contains the target account data\n\n"
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
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system", 
                "content": "You are a data analysis assistant that specializes in identifying banking data structures. Always respond with ONLY the requested JSON format."
            },
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )

    response_content = response.choices[0].message.content.strip()
    result = json.loads(response_content)
    
    if "target_sheet" not in result:
        raise ValueError(f"No valid 'target_sheet' found in the response.\nResponse: {response_content}")
    
    target_sheet = result["target_sheet"]
    confidence = result.get("confidence", 0)
    reasoning = result.get("reasoning", "No reasoning provided")
    
    if target_sheet not in sheet_names:
        raise ValueError(f"Identified sheet '{target_sheet}' not found in the Excel file.")
    
    print(f"  Identified target sheet: {target_sheet} (confidence: {confidence:.2f})")
    print(f"  Reasoning: {reasoning}")
    
    return target_sheet, confidence, reasoning

def identify_column(df, target_column, historical_mappings=None):
    """
    Use OpenAI to identify which column in the dataframe corresponds to the given target column
    
    Args:
        df: DataFrame to analyze
        target_column: TargetColumn object containing metadata
        historical_mappings: Optional dictionary of historical mappings
    """
    sample_data = df.head(2).to_dict(orient="records")
    
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
        raise ValueError(f"No valid '{target_column.name}' column found in the response. Response: {response_content}")

    if guessed_column not in df.columns:
        raise ValueError(f"Guessed column '{guessed_column}' was not found in the dataframe columns.")

    return guessed_column

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
            historical_mappings = {col.name: [] for col in TARGET_COLUMNS}

    # Initialize mappings for columns that don't have entries yet
    for column in TARGET_COLUMNS:
        historical_mappings.setdefault(column.name, [])
    
    column_mappings = {}
    
    # Process each target column
    for column in TARGET_COLUMNS:
        try:
            guessed_column = identify_column(df, column, historical_mappings)
            
            # Update historical mappings
            if guessed_column not in historical_mappings[column.name]:
                historical_mappings[column.name].append(guessed_column)
            
            column_mappings[guessed_column] = column.name
            print(f"  Mapping '{guessed_column}' to '{column.name}'")
        except Exception as e:
            print(f"  Error identifying {column.name}: {e}")
    
    # Save updated mappings
    with open("account_mapping.json", "w") as f:
        json.dump(historical_mappings, f, indent=2)
    
    # Rename the columns
    if column_mappings:
        df = df.rename(columns=column_mappings)
    
    return df

def process_excel_file(excel_file):
    """
    Process a single Excel file: identify target sheet and format columns
    """
    try:
        # Step 1: Identify which sheet contains the target data
        target_sheet, confidence, reasoning = identify_target_sheet(excel_file)
        
        # Step 2: Load the identified sheet
        print(f"\nFormatting columns in sheet '{target_sheet}'...")
        df = pd.read_excel(excel_file, sheet_name=target_sheet)
        
        # Step 3: Format the columns in the sheet
        formatted_df = format_dataframe_columns(df)
        
        # Step 4: Save the formatted data to a new Excel file
        output_dir = "processed_excel"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        filename = os.path.basename(excel_file)
        output_path = os.path.join(output_dir, f"formatted_{filename}")
        
        formatted_df.to_excel(output_path, index=False)
        print(f"  Saved formatted data to {output_path}")
        
        return {
            "file": excel_file,
            "target_sheet": target_sheet,
            "confidence": confidence,
            "reasoning": reasoning,
            "output_file": output_path,
            "success": True
        }
    
    except Exception as e:
        print(f"Error processing {excel_file}: {e}")
        return {
            "file": excel_file,
            "success": False,
            "error": str(e)
        }

def main():
    # Load historical column variations into the target column objects
    load_historical_variations()
    
    # Find all Excel files
    excel_files = glob.glob(os.path.join("excel_data", "*.xlsx"))
    if not excel_files:
        print("No Excel files found in the excel_data directory.")
        return
    
    print(f"Found {len(excel_files)} Excel files to process.")
    
    # Process each Excel file
    results = []
    for excel_file in excel_files:
        result = process_excel_file(excel_file)
        results.append(result)
    
    # Save processing results
    output_dir = "processed_excel"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    with open(os.path.join(output_dir, "processing_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    success_count = sum(1 for r in results if r.get("success", False))
    print(f"\nProcessing complete: {success_count}/{len(results)} files successfully processed")
    print(f"Results saved to {output_dir}/processing_results.json")

if __name__ == "__main__":
    main() 