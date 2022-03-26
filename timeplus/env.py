import sys
import os
import requests
from requests.structures import CaseInsensitiveDict

from timeplus.base import Base
from loguru import logger

log_level = os.environ.get("GLUON_LOG_LEVEL", "INFO")

logger.remove()

logger.add(
    sys.stdout,
    colorize=True,
    format="{time} - {level} - {message}",
    level=log_level,
)
logger.add("gluon.log", rotation="500 MB", level=log_level)


class Env(Base):
    _envs = None
    _current_env = None

    def __init__(self):
        Base.__init__(self)
        self.host("localhost")
        self.port("8000")
        self.schema("http")

        self.auth0_domain = "timeplus.us.auth0.com"
        self.audience("https://timeplus.us.auth0.com/api/v2/")
        self.grant_type = "client_credentials"

        self._headers = CaseInsensitiveDict()
        self._headers["Accept"] = "application/json"
        self._headers["Content-Type"] = "application/json"
        self._http_timeout = 10
        Env.add(self)
        self._logger = logger

    @classmethod
    def add(cls, env):
        if cls._envs is None:
            cls._envs = []

        cls._envs.append(env)

        if len(cls._envs) == 1:
            cls._current_env = env

    @classmethod
    def current(cls):
        return cls._current_env

    @classmethod
    def setCurrent(cls, env):
        cls._current_env = env

    @classmethod
    def envs(cls):
        return cls._envs

    def host(self, *args):
        return self.prop("host", *args)

    def port(self, *args):
        return self.prop("port", *args)

    def schema(self, *args):
        return self.prop("schema", *args)

    def audience(self, *args):
        return self.prop("audience", *args)

    def base_url(self):
        return f"{self.schema()}://{self.host()}:{self.port()}/api/v1beta1"

    def headers(self):
        self._headers["Authorization"] = f"Bearer {self.access_token()}"
        return self._headers

    def token(self, *args):
        return self.prop("token", *args)

    def access_token(self, *args):
        if "token" in self._data:
            return self._data["token"]["access_token"]
        return ""

    def login(
        self,
        client_id,
        client_secret,
    ):
        url = f"https://{self.auth0_domain}/oauth/token"
        self._logger.debug("post {}", url)
        request_data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": self.audience(),
            "grant_type": self.grant_type,
        }
        headers = {"Content-Type": "application/json"}
        try:
            r = requests.post(
                url,
                json=request_data,
                headers=headers,
                timeout=self.http_timeout(),
            )
            if r.status_code < 200 or r.status_code > 299:
                self._logger.error(f"failed to login {r.status_code } {r.text}")
                raise Exception(f"failed to login {r.status_code } {r.text}")
            else:
                self._logger.debug("token has been granted")
                self.token(r.json())
                return self
        except Exception as e:
            self._logger.error("failed to get token {}", e)
            raise e

    def logout(self):
        self.token({})
        return self

    def info(self):
        url = f"{self.schema()}://{self.host()}:{self.port()}/info"
        try:
            r = requests.get(url, timeout=self.http_timeout())
            if r.status_code < 200 or r.status_code > 299:
                raise Exception(f"failed to show info {r.status_code } {r.text}")
            else:
                return r.json()
        except Exception as e:
            raise e

    def ping(self):
        url = f"{self.schema()}://{self.host()}:{self.port()}/health"
        try:
            r = requests.get(url, timeout=self.http_timeout())
            if r.status_code < 200 or r.status_code > 299:
                raise Exception(f"failed to ping {r.status_code } {r.text}")
            else:
                return r.json()
        except Exception as e:
            raise e

    def logger(self):
        return self._logger

    def http_timeout(self):
        return self._http_timeout
