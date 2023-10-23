import os
import psycopg2

# Define database connection parameters
db_params = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}


try:
    # Establish a connection to database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Create the 'movie' table
    cur.execute("""
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
    """)

    # Create the 'genre' table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS genre (
            id INTEGER PRIMARY KEY,
            name TEXT
        );
    """)

    # Create the 'movie_genre' table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_genre (
            movie_id INTEGER REFERENCES movie (id),
            genre_id INTEGER REFERENCES genre (id),
            PRIMARY KEY (movie_id, genre_id)
        );
    """)

    # Create the 'production_company' table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS production_company (
            id INTEGER PRIMARY KEY,
            name TEXT,
            origin_country TEXT
        );
    """)


    # Commit the changes to the database
    conn.commit()

    print("Tables created successfully.")

except psycopg2.Error as error:
    print("Error:", error)

finally:
    # Close the connection to database
    if conn:
        cur.close()
        conn.close()