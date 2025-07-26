# Basnya Data Engineering

Data Engineering infrastructure for the Basnya (Fable) Project, focused on collecting, processing, and analyzing NBA game data.

## Project Overview

Basnya DE is a comprehensive data engineering solution designed to collect NBA game data, process it through a robust ETL pipeline, and make it available for analytics and visualization. The system automatically fetches game data, player statistics, and boxscores from the NBA API, stores them in a structured database, transforms the data using dbt, and makes it available for visualization through Apache Superset.

## Project Components

### PostgreSQL Database

The PostgreSQL database serves as the central data repository for the project, storing both raw and processed NBA data:

- **Raw Data Tables**: Store data directly from the NBA API, including:
  - Game information (leaguegamefinder__leaguegamefinderresults)
  - Player statistics (boxscoretraditionalv2__playerstats)
  - Team statistics (boxscoretraditionalv2__teamstats)
  - Bench vs. starter statistics (boxscoretraditionalv2__teamstarterbenchstats)
  
- **Operational Tables**: Track system operations and status:
  - Date processing status (date_processing_status)
  - API call logs (api_call_logs)

The database schema is designed to efficiently store NBA game data while maintaining relationships between games, teams, and players.

### Prefect Workflows

Prefect is used for workflow orchestration, managing the data collection process with reliability and monitoring:

- **Game Data Collection**: Fetches basic game information for specified date ranges
  - Automatically handles API rate limiting and retries
  - Logs all API calls for monitoring and troubleshooting
  
- **Boxscore Collection**: Fetches detailed boxscore data for games
  - Identifies games without boxscore data
  - Retrieves and stores comprehensive statistics
  
- **Processing Status Tracking**: Monitors which dates and games have been processed
  - Tracks retry attempts for failed operations
  - Ensures data completeness

Prefect workflows are designed to run on a schedule, automatically keeping the database up-to-date with the latest NBA game data.

### DBT (Data Build Tool)

DBT transforms raw NBA data into analytics-ready models through a structured transformation process:

- **Staging Models**: Clean and standardize raw data
  - Convert data types (e.g., string dates to date format)
  - Standardize column names and formats
  - Filter relevant fields
  
- **Mart Models**: Create business-oriented views of the data
  - Track data processing status and completeness
  - Calculate aggregated statistics
  - Create denormalized views for easier analysis

The DBT models follow a modular structure, making it easy to extend the transformations as new data requirements emerge.

### Apache Superset

Apache Superset provides interactive dashboards and visualizations based on the processed data:

- Player performance tracking
- Team statistics comparison
- Game outcome analysis
- System performance monitoring

## Workflow

1. Daily Prefect flows check the status of the last loaded date of games in the database
2. The flows trigger fetching of all missing game dates, players, and game statistics
3. DBT transforms the raw data:
   - Tracking data engineering system performance (processed dates and games status)
   - Updating tables with season averages of minutes, points, assists, and rebounds
4. Superset dashboards visualize the processed data for analysis

## Getting Started

See the [SERVICES.md](SERVICES.md) file for information about the services used in this project and how to access them.