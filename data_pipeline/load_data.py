import os
import json
import logging
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from tkinter import Tk, filedialog


# Define database connection parameters
db_params = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}


def select_folder():
    root = Tk()
    root.withdraw()  # Hide the main window

    # Use the file dialog to select a folder
    folder_path = filedialog.askdirectory(title="Select a folder with data to add to the database.")
    return Path(folder_path)


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


def main():
    # Prompt the user to select a folder
    json_files_dir = select_folder()  

    if not json_files_dir:
        logging.error("No folder selected. Exiting.")
        return

    if not os.listdir(json_files_dir):
        logging.error("The selected directory is empty. Exiting.")
        return

    try:
        # Load environment variables
        load_dotenv()

        with psycopg2.connect(**db_params) as conn, conn.cursor() as cur:
            for file_path in json_files_dir.glob("*.json"):
                if str(file_path).endswith(".json"):
                    with file_path.open("r", encoding="utf-8") as json_file:
                        movie_data = json.load(json_file)
                        movie_id = movie_data["id"]
                        insert_movie_data(movie_data, cur)
                        insert_movie_genre_data(movie_data, movie_id, cur)
                        insert_production_company_data(movie_data, cur)
                        logging.info(f"Inserted data from {file_path.name}")

        logging.info("Data insertion completed.")

    except psycopg2.Error as error:
        logging.error("Error: %s", error)

if __name__ == "__main__":
    # Configure logging
    log_folder = "logs"
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(log_folder, f"load_data_{timestamp}.log"), 
                                              level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    main()