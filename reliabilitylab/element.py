"""HTTP transport for the published Element application."""
import os
from urllib.parse import urlsplit

import requests

API_SUFFIX = '/api/reliability/v1'


class UncertainResponse(Exception):
    """The client cannot establish the result from this HTTP exchange."""


class ElementClient:
    def __init__(self, base_url, timeout, session=None, token=None, credentials=None):
        self.base_url = base_url.rstrip('/')
        url = urlsplit(self.base_url)
        if (url.scheme != 'https' or not url.hostname or url.username or url.password
                or url.query or url.fragment or not url.path.endswith(API_SUFFIX)):
            raise ValueError(f'Element API must be an HTTPS URL ending with {API_SUFFIX}')
        self.timeout = timeout
        self.session = session if session is not None else requests.Session()
        self.token = token
        self.credentials = credentials

    @classmethod
    def from_environment(cls, timeout):
        token = os.getenv('RELIABILITYLAB_API_TOKEN')
        client_id = os.getenv('RELIABILITYLAB_CLIENT_ID')
        secret = os.getenv('RELIABILITYLAB_CLIENT_SECRET')
        if not token and not (client_id and secret):
            raise ValueError('Set an Element Bearer token or both application client credentials')
        client = cls(os.getenv('RELIABILITYLAB_API_BASE_URL', ''), timeout, token=token,
                     credentials=(client_id, secret) if client_id and secret else None)
        if not client.token:
            client.refresh_token()
        return client

    def refresh_token(self):
        url = self.base_url[:-len(API_SUFFIX)] + '/sys/token'
        try:
            response = self.session.post(url, data={'grant_type': 'CLIENT_CREDENTIALS'},
                                         auth=self.credentials, timeout=self.timeout, allow_redirects=False)
            if response.status_code != 200:
                raise RuntimeError(f'Element token endpoint returned HTTP {response.status_code}')
            token = response.json()['id_token']
        except (requests.RequestException, ValueError, KeyError, TypeError):
            raise RuntimeError('Element token exchange failed') from None
        if not isinstance(token, str) or not token:
            raise RuntimeError('Element token endpoint returned no valid token')
        self.token = token

    def request(self, method, path, data=None):
        def send():
            headers = {'Accept': 'application/json'}
            if self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            return self.session.request(method, self.base_url + path, json=data, headers=headers,
                                        timeout=self.timeout, allow_redirects=False)

        try:
            response = send()
            if response.status_code == 401 and self.credentials:
                self.refresh_token()
                response = send()
        except requests.RequestException:
            raise UncertainResponse('HTTP_TRANSPORT_ERROR') from None
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        return response.status_code, payload if isinstance(payload, dict) else {}
