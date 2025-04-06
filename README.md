# Database Excel Processor

A Streamlit application that processes Excel files and maps them to database tables using AI-powered column matching.

## Overview

This application provides an intuitive interface for:

- Uploading Excel files with tabular data
- Automatically identifying the most relevant sheet in multi-sheet Excel files
- Intelligently mapping Excel columns to database columns
- Previewing and adjusting column mappings
- Saving processed data to SQL Server database tables
- Downloading processed data as CSV files

The system uses AI (powered by OpenAI) to intelligently identify which sheet contains the target data and to map columns between Excel sources and database destinations.

## Features

- **AI-powered Data Mapping**: Automatically identifies the right sheet and maps columns to database tables
- **Historical Mapping Memory**: Remembers previous column mappings to improve future matching
- **Flexible Database Integration**: Works with SQL Server using configurable connections
- **Interactive UI**: Preview data and adjust mappings before saving
- **Multiple Output Options**: Save to database or download as CSV

## Requirements

- Python 3.8+
- SQL Server with ODBC Driver 17 for SQL Server
- Required Python packages (see requirements.txt)

## Installation

1. Clone this repository:
   ```
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Create and activate a virtual environment (recommended):
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Create a `.env` file with your database connection details:
   ```
   DB_SERVER=your_server_name
   DB_NAME=your_database_name
   DB_USERNAME=your_username  # Optional if using Windows authentication
   DB_PASSWORD=your_password  # Optional if using Windows authentication
   OPENAI_API_KEY=your_openai_api_key
   ```

## Usage

1. Start the application:
   ```
   streamlit run app.py
   ```

2. In the web interface:
    - Select a target database table
    - Upload an Excel file
    - Review the AI's automatic sheet and column mappings
    - Adjust mappings if needed
    - Save to database or download as CSV

## Configuration

### Database Tables

Available tables are defined in the `models.py` file under `AVAILABLE_TABLES`. Add your database tables to this list to make them selectable in the UI.

### Default Column Definitions

If database column definitions cannot be loaded, the application falls back to default column definitions defined in `models.py` under `DEFAULT_TARGET_COLUMNS`.

### Historical Column Variations

The system learns from previous mappings and stores them in `historical_column_variations.json`. This file is automatically created and updated as you use the application.

## Project Structure

- `app.py`: Main Streamlit application
- `controller.py`: Core logic for processing data and managing state
- `db_utils.py`: Database utilities using the DatabaseUtils class
- `ai_utils.py`: AI-powered utilities for column and sheet identification
- `models.py`: Data models and default configurations

## License

[Your license information here]
