import concurrent.futures
import json
import os
import glob
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI()

# Target column types we're looking for
TARGET_COLUMNS = ['account_id', 'balance', 'open_date', 'status', 'email', 
                  'phone', 'address', 'customer_name', 'last_activity']

def identify_target_sheet(excel_file):
    """
    Use OpenAI to identify which sheet in an Excel file contains the target data
    """
    print(f"\nProcessing {excel_file}...")
    
    # Load the Excel file
    xl = pd.ExcelFile(excel_file)
    sheet_names = xl.sheet_names
    print(f"  Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
    
    # Load historical column mappings if available
    try:
        with open("column_variations.json", "r") as f:
            column_variations = json.load(f)
    except Exception as e:
        print(f"  Warning: Could not load column mappings: {e}")
        column_variations = {col: [] for col in TARGET_COLUMNS}
    
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
    
    # Create prompt for OpenAI
    prompt = (
        "You are tasked with identifying which sheet in an Excel file contains specific banking account data.\n\n"
        "Here are the sheets in the file and their column names and sample data:\n\n"
    )
    
    for sheet_name, data in sheet_data.items():
        prompt += f"Sheet name: {sheet_name}\n"
        prompt += f"Columns: {json.dumps(data['columns'])}\n"
        prompt += f"Sample data: {json.dumps(data['sample'], indent=2)}\n\n"
    
    prompt += (
        "The target sheet should contain columns related to banking account information such as:\n"
        f"{', '.join(TARGET_COLUMNS)}\n\n"
        "Historical variations of these column names include:\n"
        f"{json.dumps(column_variations, indent=2)}\n\n"
        "INSTRUCTIONS:\n"
        "- Analyze each sheet's column names and data patterns\n"
        "- Look for columns that semantically match the target banking data concepts\n"
        "- Consider similarity to the historical column name variations provided\n"
        "- Identify which sheet most likely contains the target account data\n\n"
        "RESPONSE FORMAT:\n"
        "Respond with ONLY a valid JSON object in the following format:\n"
        "```\n"
        "{\n"
        '  "target_sheet": "sheet_name_here",\n'
        '  "confidence": 0.55,\n'
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
    
    if target_sheet not in sheet_names:
        raise ValueError(f"Identified sheet '{target_sheet}' not found in the Excel file.")
    
    print(f"  Identified target sheet: {target_sheet}")
    print(f"  Confidence: {confidence:.2f}")

    return {
        "file": excel_file,
        "target_sheet": target_sheet,
        "confidence": confidence,
    }

def main():
    # Create output directory if it doesn't exist
    output_dir = "excel_analysis"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Find all Excel files
    excel_files = glob.glob(os.path.join("excel_data", "*.xlsx"))
    if not excel_files:
        print("No Excel files found in the excel_data directory.")
        return
    
    print(f"Found {len(excel_files)} Excel files to process.")
    
    # Process each Excel file
    results = []
    for excel_file in excel_files:
        try:
            result = identify_target_sheet(excel_file)
            results.append(result)
        except Exception as e:
            print(f"Error processing {excel_file}: {e}")
    
    # Save results to a JSON file
    output_file = os.path.join(output_dir, "sheet_identification_results.json")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {output_file}")
    
    # Print summary
    print("\nSummary of results:")
    for result in results:
        print(f"File: {os.path.basename(result['file'])}, Target Sheet: {result['target_sheet']}, Confidence: {result['confidence']:.2f}")

if __name__ == "__main__":
    main() 