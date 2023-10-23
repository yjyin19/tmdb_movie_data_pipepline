from fetch_data import send_request, api_key, s3_bucket_name
import os
import boto3
import pandas as pd

# Initialize the S3 client
s3 = boto3.client('s3')

def main():
    """This function is used to fetch data that's only needed to be fetched once."""
    if api_key is None:
        print("API key not found. Please set it and try again.")
        return
    elif s3_bucket_name is None:
        print("S3_BUCKET_NAME environment variable is not set. Please set it and try again.")
        return

    s3_data_folder = f"TMDB_movie_data_raw/"
    local_data_folder = s3_data_folder.rstrip('/').split('/')[-1]
    if not os.path.exists(local_data_folder):
        os.mkdir(local_data_folder)

    # Get the list of movie genres
    movie_genre_list_url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
    movie_genre_list = send_request(movie_genre_list_url)
    
    genres_df = pd.DataFrame(movie_genre_list["genres"])
    
    movie_genre_filename = f"TMDB_movie_data_movie_genre.csv"
    movie_genre_file = os.path.join(local_data_folder, movie_genre_filename)
    genres_df.to_csv(movie_genre_file, index=False)  # Set index=False to avoid writing row numbers

    s3.upload_file(movie_genre_file, s3_bucket_name, os.path.join(s3_data_folder, movie_genre_filename))
 

if __name__ == '__main__':
    main()