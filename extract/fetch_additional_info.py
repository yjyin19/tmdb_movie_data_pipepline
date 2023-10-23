from fetch_data import send_request, save_json_to_local, api_key, s3_bucket_name
import os
import boto3

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
    
    movie_genre_file = os.path.join(local_data_folder, f"TMDB_movie_data_movie_genre.json")
    save_json_to_local(movie_genre_list, movie_genre_file)

    s3.upload_file(movie_genre_file, s3_bucket_name, os.path.join(s3_data_folder, f"TMDB_movie_data_movie_genre.json"))
 

if __name__ == '__main__':
    main()