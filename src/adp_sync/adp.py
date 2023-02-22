import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

SERVICE_ROOT = "https://api.adp.com"


def authorize(client_id, client_secret, cert_filepath, key_filepath):
    # instantiate ADP client
    token_url = "https://accounts.adp.com/auth/oauth/v2/token"
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    client = BackendApplicationClient(client_id=client_id)
    session = OAuth2Session(client=client)
    session.cert = (cert_filepath, key_filepath)

    # authorize ADP client
    token_dict = session.fetch_token(token_url=token_url, auth=auth)
    access_token = token_dict.get("access_token")
    session.headers["Authorization"] = f"Bearer {access_token}"

    return session


def get_record(session, endpoint, querystring={}, id=None, object_name=None):
    url = f"{SERVICE_ROOT}{endpoint}"
    if id:
        url = f"{url}/{id}"

    r = session.get(url, params=querystring)

    if r.status_code == 204:
        return None

    if r.status_code == 200:
        data = r.json()
        object_name = object_name or endpoint.split("/")[-1]
        return data.get(object_name)
    else:
        r.raise_for_status()


def get_all_records(session, endpoint, querystring={}, object_name=None):
    querystring["$skip"] = querystring.get("$skip", 0)
    all_data = []

    while True:
        data = get_record(session, endpoint, querystring, object_name=object_name)

        if data is None:
            break
        else:
            all_data.extend(data)
            querystring["$skip"] += 50

    return all_data


def post(session, endpoint, subresource, verb, payload):
    url = f"{SERVICE_ROOT}{endpoint}.{subresource}.{verb}"
    try:
        r = session.post(url, json=payload)
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        if r.status_code in [403, 404]:
            response = r.json().get("response")
            application_code = response.get("applicationCode")
            raise requests.exceptions.HTTPError(
                f"\t{application_code.get('code')}: "
                f"{application_code.get('message')}\n"
                f"\t{response.get('resourceUri').get('href')}"
            )
        else:
            resource_messages = r.json().get("confirmMessage").get("resourceMessages")
            process_messages = next(
                iter([m.get("processMessages") for m in resource_messages]), []
            )
            formatted_message = f"\t{url}\n\t{payload}\n\n"
            for m in process_messages:
                m.get("processMessages")
                formatted_message += (
                    f"\t{r.status_code} - {r.reason}: "
                    f"{m.get('userMessage').get('messageTxt')}"
                )
            raise requests.exceptions.HTTPError(formatted_message)
    except Exception as xc:
        raise xc