import gzip
import json
import os
import pathlib
import traceback

from google.cloud import storage

from adp_sync import adp, email


def main():
    # instantiate ADP client
    adp_client = adp.authorize(
        os.getenv("CLIENT_ID"),
        os.getenv("CLIENT_SECRET"),
        os.getenv("CERT_FILEPATH"),
        os.getenv("KEY_FILEPATH"),
    )

    # instantiate GCS client
    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(os.getenv("GCS_BUCKET_NAME"))

    # define endpoint variables
    endpoint = "/hr/v2/workers"
    table_name = endpoint.replace("/", "_")
    print(endpoint)

    data_path = pathlib.Path(__file__).absolute().parent / "data" / table_name
    data_path.mkdir(parents=True, exist_ok=True)

    data_file = data_path / f"{table_name}.json.gz"

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
        email.send_email(
            subject="ADP Extract Error", body=f"{xc}\n\n{traceback.format_exc()}"
        )
