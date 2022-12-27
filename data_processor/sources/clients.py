import requests
import settings

from os.path import exists
from requests.exceptions import HTTPError
from typing import Union

from tracers import endpoint_tracer, response_tracer


class BackendAPIClient:

    def __init__(self):
        # When running the data processor service in a development mode,
        # it won't require the Backend access token because we will use
        # downloaded data data from `.csv` files.
        if settings.DEV_MODE:
            return

        self._FILE_ACCESS_TOKEN = '.backend.access_token.tmp'
        self._ACCESS_TOKEN = self._get_access_token()

    def _get_access_token(self) -> str:
        """
        Reads the user's access token from local file and validates it.
        If absent or invalid, it generates a new token and writes it to the temp file.

        Returns the user's access token.
        """

        token = self._read_access_token()
        if token and self._validate_access_token(token):
            return token

        return self._login()

    def _login(self) -> str:
        """
        Login to the Backend app using predefine service account
        and writes the access token to the temp file.
        """

        method = 'POST'
        path = '/auth/login/'

        payload = {
            'username': settings.BACKEND_SERVICE_ACCOUNT,
            'password': settings.BACKEND_SERVICE_ACCOUNT_PASSWORD
        }

        response = self._auth_request(method, path, payload=payload)

        token = response.json().get('token')
        self._write_access_token(token)

        return token

    def _validate_access_token(self, token) -> bool:
        """
        Returns True if that `token` is valid.
        If we can retrieve the user profile belonging to that access `token`,
        we assume that `token` is valid.

        Normally, the Backend access token is valid for 1 day.
        """

        method = 'GET'
        path = '/accounts/me/'

        try:
            self._api_request(method, path, headers={
                "Authorization": f"Token {token}"
            })
        except HTTPError:
            return False

        return True

    def _write_access_token(self, token) -> None:
        """
        Write that Backend access `token` into disk.
        """

        with open(self._FILE_ACCESS_TOKEN, "w") as file_access_token:
            file_access_token.write(token)

    def _read_access_token(self) -> Union[str, None]:
        """
        Read access token from the temporary file.

        Returns the access token or None.
        """

        if not exists(self._FILE_ACCESS_TOKEN):
            return None

        with open(self._FILE_ACCESS_TOKEN, "r") as file_access_token:
            try:
                return file_access_token.read()
            except Exception:
                return None

    def _auth_request(self, method, path, payload={}, headers=None) -> requests.Response:

        url = settings.BACKEND_URL + path

        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        if headers:
            req_headers.update(headers)

        hooks = {'response': response_tracer} if settings.DEBUG_MODE else None
        auth = (payload.get('username'), payload.get('password'))

        response = requests.request(method, url, auth=auth, headers=req_headers, hooks=hooks)
        response.raise_for_status()

        return response

    def _api_request(self, method, path, payload=None, headers=None) -> requests.Response:

        url = settings.BACKEND_URL + path
        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Token {self._ACCESS_TOKEN}" if hasattr(self, '_ACCESS_TOKEN') else None
        }

        if headers:
            req_headers.update(headers)

        hooks = {'response': response_tracer} if settings.DEBUG_MODE else None

        response = requests.request(method, url, json=payload, headers=req_headers, hooks=hooks)
        response.raise_for_status()

        return response

    def _download(self, download_as, path, payload=None, headers=None) -> None:

        url = settings.BACKEND_URL + path
        req_headers = {
            "Accept": "*/*",
            "Authorization": f"Token {self._ACCESS_TOKEN}" if hasattr(self, '_ACCESS_TOKEN') else None
        }

        if headers:
            req_headers.update(headers)

        chunk_size = 4096
        hooks = {'response': endpoint_tracer} if settings.DEBUG_MODE else None

        with requests.post(url, data=payload, headers=req_headers, stream=True, hooks=hooks) as req:
            with open(download_as, 'wb') as file:
                # Writes response data in chunk
                for chunk in req.iter_content(chunk_size):
                    if not chunk:
                        continue

                    file.write(chunk)


class TherapistAPI(BackendAPIClient):

    def __init__(self) -> None:
        super().__init__()

        self._BE_THERAPISTS_FILE = '.be_therapists.csv.tmp'

    def download_data(self, format='csv') -> None:
        """
        Downloads Therapist data from Backend in CSV format.
        """
        if format not in ['json', 'csv']:
            raise ValueError(f'{format} is invalid format.')

        path = '/organizations/therapists/export/'
        payload = {'format': format}

        self._download(self._BE_THERAPISTS_FILE, path=path, payload=payload)



class InteractionAPI(BackendAPIClient):

    def __init__(self) -> None:
        super().__init__()

        self._BE_THER_INTERACTIONS_FILE = '.be_ther_interactions.csv.tmp'

    def download_data(self, format='csv') -> None:
        """
        Downloads Therapist Interaction data from Backend in CSV format.
        """
        if format not in ['json', 'csv']:
            raise ValueError(f'{format} is invalid format.')

        path = '/organizations/therapists/interactions/export/'
        payload = {'format': format}

        self._download(self._BE_THER_INTERACTIONS_FILE, path=path, payload=payload)
