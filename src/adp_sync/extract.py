import gzip
import json
import os
import pathlib
import traceback

# from datarobot.utilities import email
from google.cloud import storage

from adp_sync import adp

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
CERT_FILEPATH = os.getenv("CERT_FILEPATH")
KEY_FILEPATH = os.getenv("KEY_FILEPATH")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def main():
    # instantiate ADP client
    adp_client = adp.authorize(CLIENT_ID, CLIENT_SECRET, CERT_FILEPATH, KEY_FILEPATH)

    # instantiate GCS client
    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(GCS_BUCKET_NAME)

    # define endpoint variables
    endpoint = "/hr/v2/workers"
    table_name = endpoint.replace("/", "_")
    print(f"{endpoint}")

    data_path = PROJECT_PATH / "data" / table_name
    data_file = data_path / f"{table_name}.json.gz"
    if not data_path.exists():
        data_path.mkdir(parents=True)
        print(f"\tCreated {'/'.join(data_path.parts[-3:])}...")

    querystring = {
        "$select": ",".join(
            [
                "worker/associateOID",
                "worker/person/preferredName",
                "worker/person/legalName",
                "worker/person/customFieldGroup",
                "worker/businessCommunication/emails",
                "worker/customFieldGroup",
                "worker/workerDates",
            ]
        ),
        "$skip": 0,
    }

    all_data = adp.get_all_records(adp_client, endpoint, querystring)

    # save to json.gz
    with gzip.open(data_file, "wt", encoding="utf-8") as f:
        json.dump(all_data, f)
    print(f"\tSaved to {'/'.join(data_file.parts[-4:])}!")

    # upload to GCS
    destination_blob_name = "adp/" + "/".join(data_file.parts[-2:])
    blob = gcs_bucket.blob(destination_blob_name)
    blob.upload_from_filename(data_file)
    print(f"\tUploaded to {destination_blob_name}!")


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
        # email_subject = "ADP Extract Error"
        # email_body = f"{xc}\n\n{traceback.format_exc()}"
        # email.send_email(subject=email_subject, body=email_body)