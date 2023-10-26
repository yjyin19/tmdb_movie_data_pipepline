import os
import requests
import boto3
import pandas as pd
import logging
import argparse
from fetch_data import api_key, s3_bucket_name
from dotenv import load_dotenv
from datetime import datetime


def fetch_genre_data():
    """Fetch movie genres from the TMDB API."""
    movie_genre_list_url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
    response = requests.get(movie_genre_list_url, params={'api_key': api_key})
    if response.status_code == 200:
        return response.json()
    else:
        return None


def parse_genre_data(data):
    """Save movie genres data to a local CSV file."""
    genre_df = pd.DataFrame(data["genres"])
    return genre_df


def main(output_folder):
    """This function is used to fetch data that's only needed to be fetched once."""
    
    # Load environment variables
    load_dotenv()

    if api_key is None:
        logging.error("API key not found. Please set it and try again.")
        return
    elif s3_bucket_name is None:
        logging.error("S3_BUCKET_NAME environment variable is not set. Please set it and try again.")
        return

    s3_data_folder = output_folder + "/"
    local_data_folder = output_folder
    if not os.path.exists(local_data_folder):
        os.mkdir(local_data_folder)

    genre_df = parse_genre_data(fetch_genre_data())
    
    if genre_df is not None:
        movie_genre_filename = f"TMDB_movie_data_movie_genre.csv"
        movie_genre_file = os.path.join(local_data_folder, movie_genre_filename)
        genre_df.to_csv(movie_genre_file, index=False)

        s3.upload_file(movie_genre_file, s3_bucket_name, os.path.join(s3_data_folder, movie_genre_filename))
        logging.info("Movie genre data fetched and uploaded successfully.")
    else:
        logging.error("No genre data fetched.")
 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch and upload movie genre data to S3")
    parser.add_argument("--output_folder", help="S3 folder to store data", default="TMDB_movie_data_raw")
    args = parser.parse_args()

    # Configure logging
    log_folder = "logs"
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(filename=os.path.join(log_folder, f"fetch_genre_data_{timestamp}.log"),
                         level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    main(args.output_folder)