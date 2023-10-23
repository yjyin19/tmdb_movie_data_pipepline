# TMDB movie data pipeline project

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Prerequisites](#prerequisites)
- [Future Plans](#future-plans)

---

## Overview

The TMDB Movie data pipeline project is designed to fetch, transform, and load movie-related data from the TMDB API into a PostgreSQL database. The project includes a set of Python scripts to accomplish these tasks.

---

## Project Structure

- **scripts**: This folder contains all the ETL scripts.
  - `fetch_data.py`: Fetches movie data from the TMDB API and save the data to AWS S3.
  - `load_data.py`: Loads movie data from JSON files into the database.
  - `fetch_genre_data.py`: Fetches movie genre data from the TMDB API and save the data to AWS S3.
  - `load_genre_data.py`: Loads genre data from a CSV file into the database.
  - `create_tables.py`: Creates the database tables.

---

## Usage

To run the data pipeline, follow these steps:

1. Set up your PostgreSQL database and configure the connection parameters using environment variables.

2. Execute the scripts in the following order:
   - `create_tables.py` to create the database tables.
   - `fetch_genre_data.py` to fetch movie genre data.
   - `load_genre_data.py` to load genre data from a CSV file into the database.
   - `fetch_data.py` to fetch movie data from the TMDB API.
   - `load_data.py` to load movie data from JSON files into the database.

3. Review the logs for any errors or issues during the process.

---

## Prerequisites

- Python 3.11.
- Required Python libraries (see `requirements.txt`).
- An AWS account with access to S3.
- PostgreSQL database server.

---

## Future Plans

Ideas to extend the project on the following aspects:

- **Cloud Migration**: I intend to move the data transformation and loading part to AWS Cloud. This will involve leveraging AWS Lambda for data processing and AWS RDS for database storage.

- **Scheduled Data Updates**: I intend to set up a scheduling mechanism to fetch data from the TMDB API on a daily basis, ensuring that the database is always up to date with the latest movie information.

- **Automated Data Pipeline**: The goal is to further automate the data pipeline, making it more robust and efficient.

- **Data Analysis**: The plan is to perform data analysis on the data and create dashboards.
