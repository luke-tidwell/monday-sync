# monday-sync

Ingests structured board data from Monday.com and writes it to SQL Server tables for reporting and tracking.

---

## Why This Project

Modern asset management and compliance processes require seamless integration between SaaS platforms and internal databases. This project demonstrates practical skills in API integration, data normalization, and robust ETL (Extract, Transform, Load) logic—solving the real-world problem of synchronizing asset data from Monday.com with enterprise SQL systems for reporting and audit purposes.

---

## Features

- **Monday.com API Integration:** Securely fetches asset board data using environment-based credential management.
- **Data Normalization:** Processes and normalizes JSON data from Monday.com into a tabular format suitable for SQL ingestion.
- **Automated ETL:** Truncates and reloads the target SQL table with fresh data on each run.
- **Environment Variable Management:** Uses `python-dotenv` for secure and flexible configuration.
- **Error Handling:** Robust error handling for API, data processing, and SQL operations.

---

## Tech Stack

- **Python 3.7+**
- **Monday.com API** (GraphQL)
- **SQL Server** (accessed via `pyodbc`)
- **Environment Variable Management** (`python-dotenv`)
- **Pandas** (for data manipulation)

---

## How It Works

1. Loads configuration and credentials from environment variables.
2. Fetches asset data from a specified Monday.com board using the GraphQL API.
3. Handles pagination to retrieve all items from the board.
4. Processes and normalizes the JSON response into a structured format.
5. Truncates the target SQL table and inserts the processed data, including a load timestamp.

---

## Code Structure

```
undeclared-assets-api/
├── UndeclareAssetsAPI.py   # Main script: orchestrates API fetch, data processing, and SQL load
├── requirements.txt        # Python dependencies
├── .env.example            # Example environment variable file
└── README.md               # Project documentation
```

---

## Contact

For questions or support, please open an issue or contact the repository maintainer.
