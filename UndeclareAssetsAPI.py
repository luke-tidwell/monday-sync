import requests
import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("MONDAY_API_KEY")
URL = 'https://api.monday.com/v2'
BOARD_ID = os.getenv("MONDAY_BOARD_ID")

server = os.getenv("SQL_SERVER")
database = os.getenv("SQL_DATABASE")
trusted = os.getenv("SQL_TRUSTED", "Yes") # Defaults to "Yes"

conn = (
    f"Driver={{SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    f"Trusted_Connection={trusted};"
)

cursor = conn.cursor()

# Undeclared Assets board query
query = """
query {
      boards (ids: {BOARD_ID}) {
        items_page {
          cursor
          items {
            id 
            name 
            column_values {
              id
              text
            }
          }
        }
      }
    }
"""


def fetch_monday_data():
    headers = {
        'Authorization': API_KEY,
        'API-Version': os.getenv('MONDAY_API_VERSION'),
    }

    all_items = []

    # Step 1: Fetch initial page of data
    json_data = {'query': query}
    response = requests.post(URL, headers=headers, json=json_data)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

    response_json = response.json()

    # Extract initial items
    boards = response_json.get("data", {}).get("boards", [])
    if not boards:
        return []

    first_items_page = boards[0].get("items_page", {})
    all_items.extend(first_items_page.get("items", []))
    cursor = first_items_page.get("cursor")  # Get the initial cursor for pagination

    # Step 2: Fetch additional pages using `next_items_page`
    while cursor:
        paginated_query = f"""
            query {{
              next_items_page (cursor: "{cursor}") {{
                cursor
                items {{
                  id
                  name
                  column_values {{
                    id
                    text
                  }}
                }}
              }}
            }}
            """

        json_data = {'query': paginated_query}
        response = requests.post(URL, headers=headers, json=json_data)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch paginated data: {response.status_code} - {response.text}")

        response_json = response.json()
        next_page_data = response_json.get("data", {}).get("next_items_page", {})
        #print(f"Next page data structure: {next_page_data}")

        # Ensure `next_items_page` is a dictionary, not a list
        if isinstance(next_page_data, list):
            next_page_data = next_page_data[0] if next_page_data else {}

        if not isinstance(next_page_data, dict):
            raise Exception(f"Unexpected data structure: {next_page_data}")

        # Extract items and update cursor
        all_items.extend(next_page_data.get("items", []))
        cursor = next_page_data.get("cursor")  # Get next cursor (if exists)

    print(f"Total items retrieved: {len(all_items)}")
    return all_items  # Returns a complete list of all items

def process_data(json_response):
    try:
        print(f"First few entries: {json_response[:2]}")  # Debug print

        all_rows = []

        for item in json_response:  # Iterate over list items directly
            if not isinstance(item, dict):
                print(f"Unexpected item structure: {item}")
                continue

            item_id = item.get("id")
            item_name = item.get("name")

            # Access column values
            column_values = item.get("column_values", [])

            if not isinstance(column_values, list):
                print(f"Unexpected column_values structure: {column_values}")
                continue

            column_data = {
                col.get("id"): col.get("text", None) for col in column_values if isinstance(col, dict)
            }

            # Add a row with all relevant data
            row = {
                "item_id": item_id,
                "item_name": item_name,
                **column_data  # Add column values dynamically
            }
            all_rows.append(row)

        print(f"Total rows processed: {len(all_rows)}")
        return all_rows
    except Exception as e:
        print(f"An error occurred while processing data: {e}")
        return []

def insert_into_sql(data, cursor, conn):

    try:
        # Step 1: Truncate the table
        cursor.execute('TRUNCATE TABLE dbo.Undelcared_Assets_Monday_API')
        print(f"Table dbo.Undelcared_Assets_Monday_API truncated successfully.")

        # Step 2: Insert data with Load_Date
        for row in data:
            # Extract fields from processed data with the if logic for empty strings
            site_id = row.get("item_name") if row.get("item_name") != "" else None  # INT
            approver = row.get("person") if row.get("person") != "" else None  # NVARCHAR(255)
            status = row.get("status") if row.get("status") != "" else None  # NVARCHAR(255)
            start_date = row.get("date4") if row.get("date4") != "" else None  # DATE
            end_date = row.get("date__1") if row.get("date__1") != "" else None  # DATE
            untracked_asset_values = row.get("numbers__1") if row.get("numbers__1") != "" else None  # FLOAT
            purpose = row.get("text3__1") if row.get("text3__1") != "" else None  # NVARCHAR(2000)

            # Define the SQL INSERT statement
            query = '''
                   INSERT INTO dbo.Undelcared_Assets_Monday_API (
                       Site_ID, 
                       Approver, 
                       Status, 
                       Start_Date, 
                       End_Date, 
                       Untracked_Asset_Values, 
                       Purpose,
                       Load_Date
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
               '''

            # Execute the query with the row values
            cursor.execute(query, (site_id, approver, status, start_date, end_date, untracked_asset_values, purpose))

        # Commit the transaction
        conn.commit()
        print(f"Data successfully inserted into table: EDW.finance.Undelcared_Assets_Monday_API")

    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        print(f"An error occurred while inserting into SQL: {e}")

try:
    json_data = fetch_monday_data()
    df = process_data(json_data)
    insert_into_sql(df, cursor, conn)
    print("Data successfully inserted into SQL Server.")
except Exception as e:
    print(f"An error occurred: {e}")