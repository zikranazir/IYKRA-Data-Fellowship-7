from google.cloud import storage
from google import auth
from dotenv import load_dotenv
import urllib.request
import logging
import os

load_dotenv()
logging.basicConfig(filename="logfile.log", level=logging.DEBUG,
                    format="%(asctime)s | %(message)s", filemode="w")

credentials, project_id = auth.default()


def uploader():

    bucket_name = input("Insert Bucket Name: ")
    url = input("Insert URL: ")
    blob_name = input("Input saved file name: ")
    client = storage.Client(credentials=credentials, project=project_id)
    # client = storage.Client.from_service_account_json("credentials.json")
    try:
        file = urllib.request.urlopen(url)
        buckets = client.list_buckets()
        bucket_list = [bucket.name for bucket in buckets]
        # check bucket in storage
        logging.info("Check Bucket In Storage")
        if bucket_name not in bucket_list:
            logging.warning(f'{bucket_name} not found!')
            client.create_bucket(bucket_name)
            logging.info(f"Bucket {bucket_name} was created.")
            bucket = client.get_bucket(bucket_name)
        else:
            logging.info(f"{bucket_name} Found")
            bucket = client.get_bucket(bucket_name)

        blob = bucket.blob(blob_name)
        print("Upload File on progress")
        blob.upload_from_string(file.read())
        blob.make_public()

        print("====================================")
        print('Well Done, Upload Success!')
        print("====================================")
        print(f"Link: {blob.public_url}")
        print("====================================")

    except Exception as e:
        logging.error(e)


if __name__ == "__main__":
    uploader()

