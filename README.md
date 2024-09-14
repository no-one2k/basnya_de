# basnya_de
Data Engineering part of Basnya (Fable) Project

## Project Goal
The goal of this project is to create a scalable data engineering infrastructure that integrates a PostgreSQL database, DigitalOcean functions for fetching NBA data using the `nba_api` package, and the setup for DBT (Data Build Tool) and Apache Superset to build data pipelines and interactive dashboards for the **Basnya** project.

## Project Components
1. **PostgreSQL Database**: A PostgreSQL instance to store games and boxscore data.
2. **DigitalOcean Functions**:
   - Function to fetch NBA games for a specific date range and upsert them into the database.
   - Function to fetch boxscores for games that haven't been processed yet.
3. **DBT**: Will be used to transform raw data into analytics-ready models.
4. **Superset**: A business intelligence tool to create interactive dashboards from the processed data.

