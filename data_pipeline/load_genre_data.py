import psycopg2
import pandas as pd
import os
import logging
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


def select_file():
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")], title="Select the CSV file with genre information")
    return file_path


def load_genre_data(csv_file):
    try:
        with psycopg2.connect(**db_params) as conn, conn.cursor() as cur:
            insert_genre_sql = "INSERT INTO genre (id, name) VALUES (%s, %s)"
            df = pd.read_csv(csv_file)

            for _, row in df.iterrows():
                cur.execute(insert_genre_sql, (row['id'], row['name']))

        logging.info("Data insertion completed.")

    except psycopg2.Error as error:
        logging.error("Error: %s", error)


if __name__ == "__main__":
    # Configure logging
    log_folder = "logs"
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(log_folder, f"load_genre_data_{timestamp}.log"), 
                        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    csv_file = select_file()

    if not csv_file:
        logging.warning("No CSV file selected. Exiting.")
    else:
        try:
            load_genre_data(csv_file)
        except psycopg2.Error as error:
            logging.error("Database connection error: %s", error)