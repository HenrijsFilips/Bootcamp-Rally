# Rally Management System

A Python application for managing rally racing teams, cars, races, and results using Snowflake as the database backend.

## Project Structure

- `snowflake_db.py`: Database connection and transaction management
- `rally_data_access.py`: Data access module for rally operations
- `streamlit_app.py`: Streamlit web interface
- `quick_test.py`: Test script to demonstrate functionality

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables in `.env`:
   - SNOWFLAKE_USER
   - SNOWFLAKE_PASSWORD
   - SNOWFLAKE_ACCOUNT
   - SNOWFLAKE_WAREHOUSE
   - SNOWFLAKE_ROLE
   - SNOWFLAKE_DATABASE

## Usage

- Run tests: `python quick_test.py`
- Run web app: `streamlit run streamlit_app.py`