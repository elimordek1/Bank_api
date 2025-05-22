# Bank API ETL Project

## Overview
This project is an ETL (Extract, Transform, Load) pipeline for aggregating and processing transaction and currency data from two major Georgian banks: **Bank of Georgia (BOG)** and **TBC Bank**. The pipeline fetches transactions and daily currency rates, processes them, and stores the results in a local SQLite database (`bank_data.db`).

## API Documentation
- [BOG API Documentation](https://api.bog.ge/docs/en/bonline/rates/commercial-currency)
- [TBC Bank API Documentation](https://developers.tbcbank.ge/docs/api-overview)

## Features
- **Automated ETL**: Extracts transactions and currency rates from BOG and TBC APIs.
- **Data Transformation**: Cleans and standardizes data for analysis.
- **SQLite Storage**: Loads processed data into a local SQLite database.
- **Extensible**: Modular design for easy addition of new banks or data sources.
- **Test Coverage**: Includes unit tests for all major modules.

## Directory Structure
```
Bank_api/
└── src/
    └── bank_api/
        ├── api/         # Bank API integrations (BOG, TBC)
        ├── config/      # Configuration files (if any)
        ├── data/        # Data processing and currency modules
        ├── tests/       # Unit tests
        ├── utils/       # Utility functions
        └── main.py      # Main ETL entry point
```

## Installation
1. **Clone the repository:**
   ```sh
   git clone <your-repo-url>
   cd Bank_api
   ```
2. **Create a virtual environment:**
   ```sh
   python -m venv .venv
   source .venv/Scripts/activate  # On Windows
   # or
   source .venv/bin/activate      # On Unix/Mac
   ```
3. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

## Usage
### Run the ETL Pipeline
The main ETL process is run via `main.py`:
```sh
python src/bank_api/main.py
```
- This will fetch transactions and currency rates for the hardcoded date range in `main.py` and write results to `bank_data.db`.
- You can modify the `start_date`, `end_date`, or currency list in `main.py` as needed.

### Database Output
- The SQLite database (`bank_data.db`) will be created in your working directory.
- It will contain tables for transactions and currency rates.

## Configuration
- **Bank account details** are expected in an Excel file (e.g., `data/Banks.xlsx`).
- API credentials and endpoints should be configured in the respective modules under `api/`.
- For production use, consider parameterizing dates and sensitive information via environment variables or config files.

## Dependencies
- Python >= 3.13
- pandas
- requests
- numpy
- openpyxl
- sqlite3 (standard library)
- See `requirements.txt` for the full list.

## Error Handling & Robustness
- The ETL pipeline and all modules are designed to handle real-world data issues gracefully:
  - **Missing or malformed Excel files**: The system logs errors and continues without crashing.
  - **Empty or malformed API responses**: The code checks for required fields and returns empty DataFrames if data is missing or invalid.
  - **Logging**: All errors and warnings are logged for traceability.
  - **Graceful database writes**: No data is written if the DataFrame is empty, preventing corrupt or partial records.

## Testing
- Unit tests are located in `src/bank_api/tests/`.
- To run all tests:
  ```sh
  pytest src/bank_api/tests
  ```
- **Test coverage includes:**
  - ETL pipeline integration
  - BOG and TBC API modules
  - Currency data processing
  - Utility functions (e.g., reading accounts from Excel)
  - **Edge cases and robustness:**
    - Handling of empty and malformed API responses
    - Handling of missing or invalid Excel files/columns
    - Ensuring DataFrames have correct structure or are empty when data is invalid
    - Logging of all error conditions
- **All tests pass as of the latest update, confirming robust handling of business and technical requirements.**

## Contributing
Contributions are welcome! Please open issues or submit pull requests for improvements, bug fixes, or new features.

## License
This project is licensed under the MIT License.

## References
- [BOG API Documentation](https://api.bog.ge/docs/en/bonline/rates/commercial-currency)
- [TBC Bank API Documentation](https://developers.tbcbank.ge/docs/api-overview)