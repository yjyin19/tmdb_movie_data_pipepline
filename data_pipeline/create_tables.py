import os
import psycopg2
import logging
from datetime import datetime
from dotenv import load_dotenv


# Define database connection parameters
db_params = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}


# SQL statements for table creation
CREATE_MOVIE_TABLE = """
    CREATE TABLE IF NOT EXISTS movie (
        id INTEGER PRIMARY KEY,
        adult BOOLEAN,
        budget INTEGER,
        homepage TEXT,
        imdb_id TEXT,
        original_language TEXT,
        original_title TEXT,
        overview TEXT,
        popularity FLOAT,
        release_date DATE,
        revenue INTEGER,
        runtime INTEGER,
        title TEXT,
        vote_average FLOAT,
        vote_count INTEGER
    );
"""

CREATE_GENRE_TABLE = """
    CREATE TABLE IF NOT EXISTS genre (
        id INTEGER PRIMARY KEY,
        name TEXT
    );
"""

CREATE_MOVIE_GENRE_TABLE = """
    CREATE TABLE IF NOT EXISTS movie_genre (
        movie_id INTEGER REFERENCES movie (id),
        genre_id INTEGER REFERENCES genre (id),
        PRIMARY KEY (movie_id, genre_id)
    );
"""

CREATE_PRODUCTION_COMPANY_TABLE = """
    CREATE TABLE IF NOT EXISTS production_company (
        id INTEGER PRIMARY KEY,
        name TEXT,
        origin_country TEXT
    );
"""

def create_tables():
    try:
        with psycopg2.connect(**db_params) as conn, conn.cursor() as cur:
            # Create the 'movie' table
            cur.execute(CREATE_MOVIE_TABLE)

            # Create the 'genre' table
            cur.execute(CREATE_GENRE_TABLE)

            # Create the 'movie_genre' table
            cur.execute(CREATE_MOVIE_GENRE_TABLE)

            # Create the 'production_company' table
            cur.execute(CREATE_PRODUCTION_COMPANY_TABLE)

        logging.info("Tables created successfully.")
    except psycopg2.Error as error:
        logging.error("Error creating tables: %s", error)


if __name__ == "__main__":
    # Configure logging
    log_folder = "logs"
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(log_folder,f"create_tables_{timestamp}.log"), level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


    try:
        load_dotenv()
        create_tables()

    except psycopg2.Error as error:
        logging.error("Database connection error: %s", error)