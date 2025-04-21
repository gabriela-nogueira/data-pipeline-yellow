from datetime import datetime
import requests
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def download_file(dat_ref):
    """
    Downloads a file from the given URI and saves it locally as a Parquet file.

    The file is streamed in chunks and written to './temp/temp-file.parquet'.
    Logs are recorded at the start and end of the process. In case of any exception
    during the file writing, an error is logged with the exception message.

    Parameters:
        file_url (str): The URI or URL of the file to download.

    Returns:
        None
    """

    file_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dat_ref}.parquet'
    logging.info(f"Starting file download {file_url}")
    filename = file_url.split('/')[-1]
    local_filename = f'../temp/{filename}'
    response = requests.get(file_url, stream=True)
    response.raise_for_status() 

    try:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        logging.error(f'Could not download the file.')
        raise Exception(f'{e}')
    
    logging.info(f"Download completed {file_url}")

    return None


if __name__ == '__main__':
    download_file()