import pandas as pd
import glob
import os
import json
from openai import OpenAI
from openai.types.chat import ChatCompletion

os.environ['OPENAI_API_KEY'] = ''  # Put API key here

client = OpenAI()

with open("account_mapping.json", "r") as f:
    historical_mappings = json.load(f)

# Load the excel sheets into pandas dataframes
csv_files = glob.glob(os.path.join("csv_data", "*.csv"))
dataframes = [pd.read_csv(file) for file in csv_files]
dataframes = dataframes[:10]

for i, df in enumerate(dataframes):
    sample_columns = df.columns.tolist()
    sample_data = df.head(2).to_dict(orient="records")

    prompt = (
        "You are tasked with identifying the account identifier column in a dataset.\n\n"
        "Given the following information:\n"
        "1. Sample data rows (first rows of the dataframe along with column names)\n"
        "2. Historical column names that have been identified as account identifiers in the past\n\n"
        "INSTRUCTIONS:\n"
        "- Analyze the column names and data patterns in the sample rows\n"
        "- Look for columns that match patterns typical of account IDs (unique identifiers, alphanumeric codes, etc.)\n"
        "- Consider similarity to the historical account ID column names provided\n"
        "- Select the most likely account identifier column\n\n"
        "RESPONSE FORMAT:\n"
        "Respond with ONLY a valid JSON object in the following format:\n"
        "```json\n"
        "{\n"
        '  "account_id": "column_name_here"\n'
        "}\n"
        "```\n\n"
        "Do not include any explanations, additional text, or markdown formatting outside of the JSON object.\n\n"
        "Sample rows:\n"
        f"{json.dumps(sample_data, indent=2)}\n\n"
        "Historical account ID column names:\n"
        f"{json.dumps(historical_mappings.get('historical_account_ids', []))}"
    )

    response: ChatCompletion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a data analysis assistant that specializes in identifying column types in datasets. Always respond with ONLY the requested JSON format."},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )

    response_content = response.choices[0].message.content.strip()
    guessed_column = json.loads(response_content).get("account_id")

    if not guessed_column:
        raise ValueError(f"No valid 'account_id' column returned found in the response.\n{response_content}")

    if guessed_column not in df.columns:
        raise ValueError(f"guessed column was not found in the dataframe: {guessed_column}")

    df.rename(columns={guessed_column: "account_id"}, inplace=True)

    # update the mapping of columns names
    historical_mappings.setdefault("historical_account_ids", []).append(guessed_column)
    historical_mappings["historical_account_ids"] = list(set(historical_mappings["historical_account_ids"]))

    with open("account_mapping.json", "w") as f:
        json.dump(historical_mappings, f, indent=2)

    print(f"Renamed column {guessed_column} to account_id")

