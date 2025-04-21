import logging
import dotenv
import boto3
import sys
import os

dotenv.load_dotenv()

def upload_file_to_bucket(file_path, bucket_name, folder=None):
    """
    Uploads a local file to the root of the specified S3 bucket.

    The file will be uploaded using its original filename as the S3 object key.
    Logs are generated for both success and failure scenarios.

    Parameters:
        file_path (str): The full path to the local file.
        bucket_name (str): The name of the S3 bucket.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    logging.info(f"Preparing to upload file: {file_path} to bucket: {bucket_name}")

    if not os.path.exists(file_path):
        logging.error(f"File does not exist: {file_path}")
        return False

    if folder != None:
        file_name = folder + '/' + os.path.basename(file_path)
    else:
        file_name = os.path.basename(file_path)

    try:
        s3 = boto3.client("s3")
        s3.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=file_name
        )
        logging.info(f"File successfully uploaded to s3://{bucket_name}/{file_name}")
        return True

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False

    return None

if __name__ == '__main__':
    file_path = sys.argv[1]
    bucket_name = sys.argv[2]
    folder=sys.argv[3]
    upload_file_to_bucket(file_path, bucket_name, folder)