# Weather Data Collection and Aggregation

This is a sub-project of my [master thesis project](https://github.com/xolarvill/KWL).

## Project Description

- Collect historical weather data from multiple API services, store it in CSV files, and prepare it for aggregation.
- The data collected consists of daily observations for cities across China.
- The ultimate goal is to aggregate this data to the province level and year level for climate analysis.
- Hong Kong, Macau, and Taiwan are excluded.

## Program Structure

- `main.py`: The main entry point of the program. It parses command-line arguments and initiates the data collection process.
- `config.template.json`: A template for the configuration file. Copy this to `config.json` and fill in your API keys.
- `city_list.json`: Stores the list of provinces and their corresponding cities, including latitude and longitude for each city.
- `data_collection/`: Contains all scripts related to data collection.
  - `weather_data_collection.py`: The core dispatcher. It orchestrates the data collection process, managing parallel workers, handling API fallbacks, and utilizing a checkpoint system.
  - `visualcrossing.py`, `openweather.py`, `qweather.py`: Individual modules for fetching data from specific weather APIs. They are called by the core dispatcher.
  - `checkpoint_manager.py`: A robust checkpoint/resume system to track progress and handle interruptions gracefully.
  - `cache/`: Directory for caching API responses to avoid redundant requests.
- `storage/`: The destination for raw data collected from the APIs, saved as CSV files.
- `output/`: The destination for aggregated data (e.g., province-level yearly summaries).

## How to Run

The program is controlled via the command line from the project's root directory.

**1. Setup:**
First, create your configuration file from the template and add your API keys:
```bash
cp config.template.json config.json
```

**2. Execution:**
Use `python main.py` with optional arguments to run the data collection.

- **Run with default settings** (processes all provinces for default years with 5 parallel workers):
  ```bash
  python main.py
  ```

- **Run for specific provinces and years:**
  ```bash
  python main.py --provinces "浙江省" "江苏省" --years 2020 2022
  ```

- **Adjust the number of parallel workers:**
  ```bash
  python main.py --provinces "广东省" --workers 10
  ```

## Development Tasks

- [x] **1. Complete `city_list.json`**: The list of all cities and their coordinates is complete.
- [x] **2. Implement Core Dispatcher (`weather_data_collection.py`)**: The central logic for orchestrating API calls, handling fallbacks, and managing parallel execution is implemented.
- [x] **3. Implement Main Entry Point (`main.py`)**: The main script for running the application with command-line arguments is complete.
- [x] **4. Implement `visualcrossing.py`**: The module for the VisualCrossing API is fully functional with caching, error handling, and retries.
- [ ] **5. Implement `openweather.py`**: Complete the data fetching and processing logic for the OpenWeatherMap API.
- [ ] **6. Implement `qweather.py`**: Complete the data fetching and processing logic for the QWeather API.
- [ ] **7. Implement Data Aggregation Scripts**: Create scripts in `data_aggregation/` to process the raw data from `storage/` into yearly and provincial summaries in `output/`.
- [ ] **8. Update and Finalize `README.md`**: Keep the README updated as new features are added.
