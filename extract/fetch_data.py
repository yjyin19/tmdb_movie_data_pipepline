import os
import json
import requests
from datetime import datetime
from ratelimit import rate_limited
import boto3

# Get API key from environment variables
api_key = os.environ.get("TMDB_API_KEY")

# Get S3 bucket
s3_bucket_name = os.environ.get("S3_BUCKET_NAME")

# Initialize the S3 client
s3 = boto3.client('s3')


@rate_limited(50, 1)  # 50 requests per second as TMDB documentation states such a limit
def send_request(url, params={'api_key': api_key}):
    '''Sends a request to the TMDB API to get movies released within the specified dates and page number.'''

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None



def save_json_to_local(data, filename):
    '''Save the fetched data from API locally.'''

    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)



def main():
    '''This function fetches and saves data in 2 steps:
    1. Get the list of ids of movies that released on a certain date or during a date range. Default: get movies released today.
    2. Get the detail data of movies by using the list of ids from the previous step. Save the data of each movie in a seperate file locally and on AWS S3.
    '''

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
    

    # 1. Get the list of movie ids. The default is to fetch movies data updated today. User can also specify a date range to get older data.
    use_default_dates = input("Press 'y' to find movies released today. Press 'n' to specify a date range. (y/n): ").lower() == 'y'
    if use_default_dates:
        end_date = datetime.now()
        end_date_str = start_date_str = end_date.strftime("%Y-%m-%d")
    else:
        start_date = input("Enter the start date (YYYYMMDD): ")
        end_date = input("Enter the end date (YYYYMMDD): ")
        start_date_str = start_date[:4] + "-" + start_date[4:6] + "-" + start_date[6:]
        end_date_str = end_date[:4] + "-" + end_date[4:6] + "-" + end_date[6:]


    movie_list_url = "https://api.themoviedb.org/3/discover/movie"
    movie_list = []

    for i in range(1, 501):
        page_number = i
        movie_list_params = {
        'api_key': api_key,
        'page': page_number,
        'primary_release_date.gte': start_date_str,
        'primary_release_date.lte': end_date_str,
        'sort_by': 'primary_release_date.desc'}

        data = send_request(movie_list_url, movie_list_params)

        if not data.get("results"):
            break


        for item in data["results"]:
            movie_list.append(item["id"])

        if page_number == 500:
            print("Fetched 500 pages of results to generate a list of movies. There might be more results but TMDB can only show 500 pages.")


    # 2. Get the detail data of movies by using the list of ids
    if end_date_str == start_date_str:
        s3_sub_folder = f"TMDB_movie_data_raw_{end_date_str}/"
    else:
        s3_sub_folder = f"TMDB_movie_data_raw_{start_date_str}_{end_date_str}/"
    local_sub_folder = os.path.join(local_data_folder, s3_sub_folder.rstrip('/').split('/')[-1])
    if not os.path.exists(local_sub_folder):
        os.mkdir(local_sub_folder)

    for movie_id in movie_list:
        
        movie_detail_url= "https://api.themoviedb.org/3/movie/" + str(movie_id)
        movie_detail_data = send_request(movie_detail_url)

        movie_detail_file = os.path.join(local_sub_folder, f"TMDB_movie_data_raw_{movie_id}.json")
        save_json_to_local(movie_detail_data, movie_detail_file)

        s3_object_key = os.path.join(s3_data_folder + s3_sub_folder, f"TMDB_movie_data_raw_{movie_id}.json")
        s3.upload_file(movie_detail_file, s3_bucket_name, s3_object_key)


if __name__ == '__main__':
    main()