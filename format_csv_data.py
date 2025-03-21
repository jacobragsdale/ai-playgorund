import concurrent.futures
import pandas as pd
import glob
import os
import json
from openai import OpenAI
from openai.types.chat import ChatCompletion
from dotenv import load_dotenv

load_dotenv()

client = OpenAI()


def process_dataframe(df: pd.DataFrame, historical_mappings: dict, id_label: str) -> str:
    sample_data = df.head(2).to_dict(orient="records")

    prompt = (
        f"You are tasked with identifying the {id_label} column in a dataset.\n\n"
        "Given the following information:\n"
        "1. Sample data rows (first rows of the dataframe along with column names)\n"
        "2. Historical column names that have been identified as identifiers in the past\n\n"
        "INSTRUCTIONS:\n"
        "- Analyze the column names and data patterns in the sample rows\n"
        "- Look for columns that match patterns typical of identifiers (such as unique IDs, alphanumeric codes, etc.)\n"
        "- Consider similarity to the historical column names provided\n"
        f"- Select the most likely {id_label} column\n\n"
        "RESPONSE FORMAT:\n"
        f"Respond with ONLY a valid JSON object in the following format:\n"
        "```json\n"
        "{\n"
        f'  "{id_label}": "column_name_here"\n'
        "}\n"
        "```\n\n"
        "Do not include any explanations or additional text.\n\n"
        "Sample rows:\n"
        f"{json.dumps(sample_data, indent=2)}\n\n"
        "Historical identifier column names:\n"
        f"{json.dumps(historical_mappings.get(f'{id_label}', []))}"
    )

    response: ChatCompletion = client.chat.completions.create(
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
    guessed_column = json.loads(response_content).get(id_label)

    if not guessed_column:
        raise ValueError(f"No valid '{id_label}' column found in the response.\nResponse: {response_content}")

    if guessed_column not in df.columns:
        raise ValueError(f"Guessed column '{guessed_column}' was not found in the dataframe columns.")

    print(f"Renamed column '{guessed_column}' to '{id_label}'")
    return guessed_column


def main():
    # Set the columns to search for.
    id_labels = ['account_id', 'balance', 'open_date', 'status', 'email', 'phone', 'address', 'customer_name', 'last_activity']

    # Load historical mappings to pass to openai to improve performance over time
    try:
        with open("account_mapping.json", "r") as f:
            historical_mappings = json.load(f)
    except Exception:
        historical_mappings = {}

    for id_label in id_labels:
        historical_mappings.setdefault(id_label, [])

    # Load the CSV files into dataframes.
    csv_files = glob.glob(os.path.join("csv_data", "*.csv"))
    data_tuples = [(csv_file, pd.read_csv(csv_file)) for csv_file in csv_files][:50]

    # Set the directory for saving the output CSV files.
    output_dir = os.path.join("csv_data", "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for file_name, df in data_tuples:
        print(f"\nProcessing {file_name}...")
        for id_label in id_labels:
            try:
                guessed_column = process_dataframe(df, historical_mappings, id_label)
                # Update historical mappings for the id_label.
                if guessed_column not in historical_mappings[id_label]:
                    historical_mappings[id_label].append(guessed_column)
                df.rename(columns={guessed_column: id_label}, inplace=True)
            except Exception as e:
                print(f"Error processing dataframe for {id_label}: {e}")

        output_path = os.path.join('output_dir', os.path.basename("formatted_" + file_name))
        df.to_csv(output_path, index=False)
        print(f"Saved output to {output_path}")


    # Save the updated mappings.
    with open("account_mapping.json", "w") as f:
        json.dump(historical_mappings, f, indent=2)


if __name__ == "__main__":
    main()
