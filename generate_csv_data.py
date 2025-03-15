# Language: python

import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta


# --- Helper Functions ---

def random_date(start, end):
    """Return a random datetime between two datetime objects."""
    delta = end - start
    random_days = random.randrange(delta.days)
    return start + timedelta(days=random_days)


def generate_account_variants():
    """
    Programmatically generate a list of account ID column header variations.
    We'll combine several base words and suffixes with optional punctuation and spacing.
    """
    bases = ["account", "acct", "acc", "client", "customer", "bank", "ledger", "record", "profile"]
    suffixes = ["id", "ID", "number", "num", "no", "identifier", "identification"]
    variants = set()

    # add some simple ones
    basic = [
        "account", "Account", "account id", "Account ID", "acct id", "Acct ID",
        "account number", "Account Number", "acnt", "ACNT", "account#", "acct#"
    ]
    variants.update(basic)

    # combine bases and suffixes with various punctuation and spacing
    for base in bases:
        for suf in suffixes:
            for sep in [" ", "_", "-", ""]:
                v1 = f"{base}{sep}{suf}"
                v2 = f"{base.capitalize()}{sep}{suf.upper() if random.choice([True, False]) else suf}"
                variants.add(v1)
                variants.add(v2)
                # sometimes add a trailing colon
                variants.add(v1 + ":")
                variants.add(v2 + ":")

    # Add a few random extra variations with noise (e.g. extra words)
    extras = ["no.", "num.", "ID", "Code"]
    for base in bases:
        for extra in extras:
            v = f"{base} {extra}"
            variants.add(v)
            variants.add(v.capitalize())
            variants.add(v + " (unique)")

    return list(variants)


def generate_random_account_id():
    """Generate a random account id as a string of 8 digits."""
    return "".join(random.choices(string.digits, k=8))


def generate_random_email(name):
    """Generate a fake email address from a name."""
    domains = ["example.com", "sample.org", "test.net"]
    return f"{name.lower()}@{random.choice(domains)}"


def get_unique_column_name(base_name, existing_names, possibilities):
    """
    Return a random column name for the given base_name from possibilities that is not
    already in existing_names. If not possible, return base_name.
    """
    choices = possibilities.copy()
    random.shuffle(choices)
    for candidate in choices:
        if candidate not in existing_names:
            return candidate
    return base_name


# --- Main Script ---
def main():
    random.seed(42)
    np.random.seed(42)

    # Create a list of account-related column header variations.
    account_variants = generate_account_variants()
    print(f"Generated {len(account_variants)} account_id header variants.")

    # Define the mapping for non-critical (other) columns to possible header variations.
    other_columns_variations = {
        "balance": ["balance", "Balance", "current balance", "acct balance"],
        "open_date": ["open_date", "Open Date", "start date", "account open"],
        "status": ["status", "Status", "account status", "acct status"],
        "email": ["email", "Email", "e-mail", "E-mail"],
        "phone": ["phone", "Phone", "contact", "mobile"],
        "address": ["address", "Address", "street", "residence"],
        "country": ["country", "Country", "nation"],
        "zip_code": ["zip_code", "Zip Code", "postal code"],
        "account_type": ["account_type", "Account Type", "acct type", "type"],
        "customer_name": ["customer_name", "Customer", "Name", "client name"],
        "branch": ["branch", "Branch", "office", "location"],
        "last_activity": ["last_activity", "Last Activity", "recent activity", "activity date"]
    }

    num_files = 500  # Number of CSV files to generate
    num_rows = 100  # Number of rows per CSV
    output_prefix = "account_data_"

    for file_index in range(1, num_files + 1):
        # Decide how many account-related columns to include in this file.
        num_account_cols = random.randint(1, 5)
        selected_account_cols = random.sample(account_variants, num_account_cols)

        # Generate account id values for each row.
        account_ids = [generate_random_account_id() for _ in range(num_rows)]

        # Prepare the data dictionary.
        data = {}
        # Add the account-related columns (each column gets the same set of account_ids)
        for col in selected_account_cols:
            data[col] = account_ids

        # Generate data for the other columns using normalized keys.
        fixed_data = {}
        fixed_data["balance"] = np.round(np.random.uniform(10.0, 10000.0, size=num_rows), 2)
        start_date = datetime.now() - timedelta(days=5 * 365)
        end_date = datetime.now()
        fixed_data["open_date"] = [random_date(start_date, end_date).strftime("%Y-%m-%d") for _ in range(num_rows)]
        fixed_data["status"] = [random.choice(["active", "inactive"]) for _ in range(num_rows)]
        fixed_data["email"] = [generate_random_email(f"user{acc[-4:]}") for acc in account_ids]
        fixed_data["phone"] = [
            f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            for _ in range(num_rows)
        ]
        fixed_data["address"] = [
            f"{random.randint(100, 999)} {random.choice(['Main St', 'Broadway', 'First Ave', 'Maple Drive'])}" for _ in
            range(num_rows)]
        countries = ["USA", "Canada", "UK", "Australia", "Germany"]
        fixed_data["country"] = [random.choice(countries) for _ in range(num_rows)]
        fixed_data["zip_code"] = [str(random.randint(10000, 99999)) for _ in range(num_rows)]
        fixed_data["account_type"] = [random.choice(["Savings", "Checking", "Credit", "Loan"]) for _ in range(num_rows)]
        first_names = ["John", "Jane", "Alex", "Emily", "Chris", "Katie"]
        last_names = ["Smith", "Doe", "Johnson", "Brown", "Davis"]
        fixed_data["customer_name"] = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in
                                       range(num_rows)]
        fixed_data["branch"] = [f"Branch {random.randint(1, 20)}" for _ in range(num_rows)]
        recent_start = datetime.now() - timedelta(days=365)
        recent_end = datetime.now()
        fixed_data["last_activity"] = [random_date(recent_start, recent_end).strftime("%Y-%m-%d") for _ in
                                       range(num_rows)]

        # Now assign random header variations for each of the other columns,
        # ensuring we don't conflict with account columns or duplicate headers.
        used_columns = set(selected_account_cols)
        for norm_col, variants in other_columns_variations.items():
            col_name = get_unique_column_name(norm_col, used_columns, variants)
            data[col_name] = fixed_data[norm_col]
            used_columns.add(col_name)

        # Create the DataFrame.
        df = pd.DataFrame(data)

        # Shuffle the columns order so that the account-related columns and others are randomly mixed.
        cols = list(df.columns)
        random.shuffle(cols)
        df = df[cols]

        # Save the CSV to disk.
        file_name = f"{output_prefix}{file_index}.csv"
        df.to_csv('csv_data/' + file_name, index=False)
        print(f"Saved file: {file_name}")


if __name__ == '__main__':
    main()
