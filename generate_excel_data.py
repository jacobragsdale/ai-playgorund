import concurrent.futures
import json
import os.path
import random
from typing import List
from dotenv import load_dotenv

import pandas as pd
import string

from openai import OpenAI

load_dotenv()

client = OpenAI()


def get_random_date(start_year=2000, end_year=2023):
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


def get_random_email() -> str:
    user_length = random.randint(6, 12)
    user_chars = string.ascii_lowercase + string.digits + "._"
    username = ''.join(random.choices(user_chars, k=user_length))

    if username[0] in "._":
        username = random.choice(string.ascii_lowercase + string.digits) + username[1:]

    domains = ["gmail", "yahoo", "outlook", "hotmail", "example"]
    tlds = ["com", "net", "org", "io", "co"]

    email = f"{username}@{random.choice(domains)}.{random.choice(tlds)}"
    return email


def get_random_street_address():
    street_names = ["Main", "Oak", "Pine", "Maple", "Elm", "Cedar", "View", "Washington", "Lake", "Hill", "Sunset",
                    "Park"]
    street_types = ["St", "Ave", "Blvd", "Rd", "Dr", "Ln", "Ct", "Pl", "Terrace", "Way"]
    return f"{random.randint(1, 9999)} {random.choice(street_names)} {random.choice(street_types)}"


def get_random_phone_number() -> str:
    first = random.randint(2, 9)
    second = random.randint(0, 9)
    third = random.randint(0, 9)
    area_code = f"{first}{second}{third}"

    first_ex = random.randint(2, 9)
    second_ex = random.randint(0, 9)
    third_ex = random.randint(0, 9)
    exchange = f"{first_ex}{second_ex}{third_ex}"

    subscriber = random.randint(0, 9999)
    subscriber_str = f"{subscriber:04d}"

    return f"({area_code}) {exchange}-{subscriber_str}"


def get_random_value(col: str):
    if col == "account_id":
        return str(random.randint(10000, 99999))
    elif col == "balance":
        return f"{random.uniform(10, 10000):.2f}"
    elif col == "open_date":
        return get_random_date(2010, 2023)
    elif col == "status":
        return random.choice(["active", "inactive", "pending"])
    elif col == "email":
        return get_random_email()
    elif col == "phone":
        return get_random_phone_number()
    elif col == "address":
        return get_random_street_address()
    elif col == "customer_name":
        return random.choice(["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Carol White"])
    elif col == "last_activity":
        return get_random_date(2015, 2023)
    else:
        raise ValueError(f"Unhandled column: {col}")


def get_column_name_variation(column: str, previous_names: List):
    prompt = (
        "You are a data analysis assistant that specializes in identifying column types in datasets. "
        "Given the following column name, provide a human-readable variation of it. "
        "For example, 'account_id' could become 'account no.' or 'account number'. "
        "Ensure that the variation uses a mix of separators including hyphens (-), underscores (_), other separators occasionally, "
        "and vary the capitalization and punctuation to make the name appear more natural and diverse. "
        "Return ONLY the new column name with no additional text or formatting. "
        "Only provide a single column name, do not return multiple variations."
        f"Column name: {column} "
        f"Do NOT return any of the following values: {previous_names}"
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ],
    )

    response_content = response.choices[0].message.content.strip()
    return response_content


def generate_unrelated_data(num_rows, num_cols):
    """Generate random data for sheets that don't contain the target columns"""
    unrelated_columns = [
        "product_id", "price", "quantity", "category", "date_added",
        "region", "sales_rep", "discount", "rating", "in_stock",
        "supplier", "weight", "color", "material", "dimensions"
    ]

    # Select a random subset of columns
    selected_columns = random.sample(unrelated_columns, min(num_cols, len(unrelated_columns)))

    data = []
    for _ in range(num_rows):
        row = []
        for col in selected_columns:
            if "id" in col:
                row.append(str(random.randint(1000, 9999)))
            elif "price" in col or "discount" in col:
                row.append(f"{random.uniform(5, 500):.2f}")
            elif "quantity" in col or "rating" in col:
                row.append(str(random.randint(1, 100)))
            elif "date" in col:
                row.append(get_random_date())
            elif "region" in col or "category" in col or "supplier" in col:
                row.append(random.choice(["North", "South", "East", "West", "Central"]))
            elif "sales_rep" in col:
                row.append(random.choice(["Alice", "Bob", "Charlie", "David", "Eve"]))
            elif "in_stock" in col:
                row.append(random.choice(["Yes", "No"]))
            elif "color" in col:
                row.append(random.choice(["Red", "Blue", "Green", "Yellow", "Black", "White"]))
            elif "material" in col:
                row.append(random.choice(["Wood", "Metal", "Plastic", "Glass", "Fabric"]))
            else:
                row.append(f"Value-{random.randint(1, 1000)}")
        data.append(row)

    return pd.DataFrame(data, columns=selected_columns)


def main():
    num_files = 5
    num_rows = 50

    # Create directory for Excel files if it doesn't exist
    os.makedirs("excel_data", exist_ok=True)

    columns = ['account_id', 'balance', 'open_date', 'status', 'email', 'phone', 'address', 'customer_name',
               'last_activity']

    # Create dictionary with each column mapped to an empty variations list
    generated_column_variations = {col: [] for col in columns}

    def process_column(col):
        variation = get_column_name_variation(col, generated_column_variations[col])
        generated_column_variations[col].append(variation)
        return col, variation

    for file_number in range(1, num_files + 1):
        # Determine number of sheets (between 2 and 5)
        num_sheets = random.randint(2, 5)

        # Randomly select which sheet will contain the target data
        target_sheet_index = random.randint(0, num_sheets - 1)

        print(f"Generating file {file_number} with {num_sheets} sheets. Target sheet is {target_sheet_index}.")

        # Generate target data
        target_data = [[get_random_value(col) for col in columns] for _ in range(num_rows)]

        # Create a thread for each column and ask openai to generate a variation
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(process_column, columns)
        column_variations = {col: variation for col, variation in results}

        print(f"\nGenerated column variations:\n{json.dumps(column_variations, indent=2)}")

        # Create Excel file with multiple sheets
        filename = os.path.join("excel_data", f"sample_data_{file_number}.xlsx")
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            # Create sheets
            for sheet_index in range(num_sheets):
                if sheet_index == target_sheet_index:
                    # This is the target sheet with column variations
                    sheet_name = f"Sheet{sheet_index + 1}"
                    df = pd.DataFrame(target_data, columns=[column_variations[col] for col in columns])
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    # Generate random unrelated data for non-target sheets
                    sheet_name = f"Sheet{sheet_index + 1}"
                    num_cols = random.randint(3, 8)
                    df = generate_unrelated_data(num_rows, num_cols)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Successfully saved {filename}")

    # Save column variations for reference
    with open('column_variations.json', 'w') as f:
        json.dump(generated_column_variations, f, indent=2)


if __name__ == '__main__':
    main()
