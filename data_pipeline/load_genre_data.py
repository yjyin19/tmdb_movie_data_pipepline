import psycopg2
import pandas as pd
import os
from pathlib import Path

# Define database connection parameters
db_params = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}



# Path to the file containing genre information
csv_file = Path("TMDB_movie_data_raw\TMDB_movie_data_movie_genre.csv")


try:
    # Establish a connection to database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()


    insert_genre_sql = "INSERT INTO genre (id, name) VALUES (%s, %s)"

    df = pd.read_csv(csv_file)

    for index, row in df.iterrows():
        cur.execute(insert_genre_sql, (row['id'], row['name']))

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