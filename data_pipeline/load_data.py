import psycopg2
import json
import os
from pathlib import Path
from datetime import datetime

# Define database connection parameters
db_params = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}


def insert_movie_data(data, cur):
    """Insert data into the movie table"""
    insert_movie_sql = """
        INSERT INTO movie (id, adult, budget, homepage, imdb_id, original_language, original_title, 
                           overview, popularity, release_date, revenue, runtime, title, 
                           vote_average, vote_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    
    release_date = data.get("release_date")
    release_date = release_date if release_date else None
    
    if release_date:
        release_date = datetime.strptime(release_date, "%Y-%m-%d").date()

    values = (
        data["id"],
        data["adult"],
        data["budget"],
        data["homepage"],
        data["imdb_id"],
        data["original_language"],
        data["original_title"],
        data["overview"],
        data["popularity"],
        release_date,  # Use the parsed date
        data["revenue"],
        data["runtime"],
        data["title"],
        data["vote_average"],
        data["vote_count"],
    )
    
    cur.execute(insert_movie_sql, values)
    return cur.fetchone()[0]

def insert_movie_genre_data(data, movie_id, cur):
    """Insert data into the movie_genre table"""
    insert_movie_genre_sql = """
        INSERT INTO movie_genre (movie_id, genre_id)
        VALUES (%s, %s);
    """
    for genre in data["genres"]:
        cur.execute("SELECT id FROM genre WHERE name = %s;", (genre["name"],))
        genre_id = cur.fetchone()[0]
        cur.execute(insert_movie_genre_sql, (movie_id, genre_id))


def insert_production_company_data(data, cur):
    """Insert data into the into the production_company table"""
    insert_production_company_sql = """
        INSERT INTO production_company (id, name, origin_country)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """
    for company in data["production_companies"]:
        cur.execute(insert_production_company_sql, (company["id"], company["name"], company["origin_country"]))


# Directory containing JSON files
json_files_dir = Path("TMDB_movie_data_raw\TMDB_movie_data_raw_2023-10-20")



try:
    # Establish a connection to database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Loop through JSON files in the local folder
    for file_path in json_files_dir.glob("*.json"):
        if str(file_path).endswith(".json"):
            with file_path.open("r", encoding="utf-8") as json_file:
                movie_data = json.load(json_file)
                movie_id = movie_data["id"]
                insert_movie_data(movie_data, cur)
                insert_movie_genre_data(movie_data, movie_id, cur)
                insert_production_company_data(movie_data, cur)
                print(f"Inserted data from {file_path.name}")

    # Commit the changes to the database
    conn.commit()
    print("Data insertion completed.")

except psycopg2.Error as error:
    print("Error:", error)

finally:
    # Close the connection to database
    if conn:
        cur.close()
        conn.close()