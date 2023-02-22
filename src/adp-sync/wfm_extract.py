import os
import pathlib
import time
import traceback

import requests
import yaml
from datarobot.utilities import email
from google.cloud import storage

WFM_HOST_NAME = os.getenv("WFM_HOST_NAME")
WFM_APP_KEY = os.getenv("WFM_APP_KEY")
WFM_CLIENT_ID = os.getenv("WFM_CLIENT_ID")
WFM_CLIENT_SECRET = os.getenv("WFM_CLIENT_SECRET")
WFM_USERNAME = os.getenv("WFM_USERNAME")
WFM_PASSWORD = os.getenv("WFM_PASSWORD")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

PROJECT_PATH = pathlib.Path(__file__).absolute().parent
DATA_PATH = PROJECT_PATH / "data"
YAML_PATH = PROJECT_PATH / "config" / "wfm.yaml"


def get_client(host_name, app_key):
    client = requests.Session()

    client.base_url = f"https://{host_name}.mykronos.com/api"
    client.headers["appkey"] = app_key

    return client


def api_call(client, method, endpoint, **kwargs):
    url = f"{client.base_url}{endpoint}"
    try:
        response = client.request(method, url, **kwargs)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError:
        if response.status_code == 401:
            authenticate(client, client.refresh_payload)
            api_call(client, method, endpoint, **kwargs)
        else:
            error_json = response.json()
            raise requests.exceptions.HTTPError(
                f"{error_json.get('errorCode')}: {error_json.get('message')}"
            )


def get_login_payload(client_id, client_secret, username, password):
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_chain": "OAuthLdapService",
        "grant_type": "password",
        "username": username,
        "password": password,
    }


def get_refresh_payload(login_payload, refresh_token):
    refresh_payload = {
        k: v for k, v in login_payload.items() if k not in ["username", "password"]
    }

    refresh_payload["grant_type"] = "refresh_token"
    refresh_payload["refresh_token"] = refresh_token

    return refresh_payload


def authenticate(client, payload):
    client.headers["Content-Type"] = "application/x-www-form-urlencoded"
    client.headers.pop("Authorization", "")  # remove existing auth for refresh

    response = client.post(
        f"{client.base_url}/authentication/access_token",
        data=payload,
    )
    response.raise_for_status()

    client.access_token = response.json()
    client.headers["Content-Type"] = "application/json"
    client.headers[
        "Authorization"
    ] = f"Bearer {client.access_token.get('access_token')}"

    return client


def main():
    report_configs = yaml.safe_load(YAML_PATH.open("r")).get("reports")

    wfm = get_client(WFM_HOST_NAME, WFM_APP_KEY)
    login_payload = get_login_payload(
        WFM_CLIENT_ID, WFM_CLIENT_SECRET, WFM_USERNAME, WFM_PASSWORD
    )

    wfm = authenticate(wfm, login_payload)
    wfm.refresh_payload = get_refresh_payload(
        login_payload, wfm.access_token["refresh_token"]
    )

    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(GCS_BUCKET_NAME)

    reports = api_call(wfm, "GET", "/v1/platform/reports").json()
    symbolic_periods = api_call(wfm, "GET", "/v1/commons/symbolicperiod").json()
    hyperfind_queries = (
        api_call(wfm, "GET", "/v1/commons/hyperfind").json().get("hyperfindQueries")
    )

    target_executions = []
    for rc in report_configs:
        target_report = [r for r in reports if r["name"] == rc["name"]][0]
        target_period = [
            sp for sp in symbolic_periods if sp["symbolicId"] == rc["symbolic_id"]
        ][0]
        target_hyperfind = [
            hq for hq in hyperfind_queries if hq["name"] == rc["hyperfind"]
        ][0]

        target_dates_payload = {
            "where": {"currentUser": True, "symbolicPeriodId": rc["symbolic_id"]}
        }
        target_dates = api_call(
            wfm, "POST", "/v1/commons/symbolicperiod/read", json=target_dates_payload
        ).json()

        execute_endpoint = f"/v1/platform/reports/{target_report['name']}/execute"
        execute_payload = {
            "parameters": [
                {"name": "DateRange", "value": {"symbolicPeriod": target_period}},
                {"name": "DataSource", "value": {"hyperfind": target_hyperfind}},
                {
                    "name": "Output Format",
                    "value": {"key": "csv", "title": "CSV"},
                },  # undocumented: where does this come from?
            ]
        }

        execute_response = api_call(
            wfm, "POST", execute_endpoint, json=execute_payload
        ).json()

        execution_id = execute_response.get("id")
        target_executions.append(
            {
                "id": execution_id,
                "name": target_report["name"],
                "hyperfind": rc["hyperfind"],
                "symbolic_period": rc["symbolic_id"],
                "date_range": target_dates,
            }
        )

    while len(target_executions) > 0:
        report_executions = api_call(
            wfm, "GET", "/v1/platform/report_executions"
        ).json()

        for i, tex in enumerate(target_executions):
            execution = [
                rex for rex in report_executions if rex.get("id") == tex["id"]
            ][0]
            execution_status = execution.get("status").get("qualifier")

            print(
                f"{tex['name']} - "
                f"{tex['hyperfind']} - "
                f"{tex['symbolic_period']}:\t{execution_status}"
            )
            if execution_status == "Completed":
                print(f"\tDownloading {tex['name']} - {tex['symbolic_period']}...")
                report_file = api_call(
                    wfm, "GET", f"/v1/platform/report_executions/{tex['id']}/file"
                )

                # save as file
                file_dir = DATA_PATH / tex["name"]
                if not file_dir.exists():
                    print(f"\tCreating {file_dir}...")
                    file_dir.mkdir(parents=True)

                file_path = file_dir / (
                    f"{tex['name']}-"
                    f"{tex['hyperfind'].replace(' ', '')}-"
                    f"{tex['date_range']['begin']}.csv"
                )
                print(f"\tSaving to {file_path}...")
                with file_path.open("w+") as f:
                    f.write(report_file.text)

                # upload to GCS
                fpp = file_path.parts
                destination_blob_name = (
                    f"adp/" f"{'/'.join(fpp[fpp.index('data') + 1:])}"
                )
                blob = gcs_bucket.blob(destination_blob_name)
                blob.upload_from_filename(file_path)
                print(f"\tUploaded to {blob.public_url}!")

                del target_executions[i]

        time.sleep(8)


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
        email_subject = "ADP WFM Extract Error"
        email_body = f"{xc}\n\n{traceback.format_exc()}"
        email.send_email(subject=email_subject, body=email_body)