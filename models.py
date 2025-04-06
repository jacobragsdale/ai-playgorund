from dataclasses import dataclass, field
from typing import List


@dataclass
class TargetColumn:
    """Class for defining target columns and their properties"""
    name: str  # Standard column name
    data_type: str  # Data type (string, number, date, etc.)
    description: str  # Description of what this column represents
    examples: List[str] = field(default_factory=list)  # Example values for this column
    historical_variations: List[str] = field(default_factory=list)  # Known variations of column names


# Default target columns to use when database columns aren't available
DEFAULT_TARGET_COLUMNS = [
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
        name="customer_name",
        data_type="string",
        description="Full name of the customer or account holder",
        examples=["John Doe", "Jane Smith", "Acme Corporation"]
    )
]

# List of available database tables
AVAILABLE_TABLES = [
    {"schema": "dbo", "name": "Accounts"},
    {"schema": "dbo", "name": "Customers"},
    {"schema": "sales", "name": "Transactions"}
    # Add more tables as needed
]
