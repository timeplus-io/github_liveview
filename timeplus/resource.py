import requests

from timeplus.base import Base
from timeplus.env import Env


class ResourceBase(Base):
    _resource_name = "resource"

    def __init__(self, env=None):
        Base.__init__(self)
        if env is None:
            env = Env.current()
        self._headers = env.headers()
        self._base_url = env.base_url()
        self._env = env
        self._logger = env.logger()

    def create(self):
        url = f"{self._base_url}/{self._resource_name}/"
        self._logger.debug("post {}", url)
        try:
            r = requests.post(
                f"{self._base_url}/{self._resource_name}/",
                json=self.data(),
                headers=self._headers,
                timeout=self._env.http_timeout(),
            )
            if r.status_code < 200 or r.status_code > 299:
                self._logger.error(
                    "failed to create {} {}",
                    self._resource_name,
                    r.text,
                )
            else:
                self._logger.debug("source {} has been created", self._resource_name)
                self._data = r.json()
        except Exception as e:
            self._logger.error("failed to create {} {}", self._resource_name, e)
        finally:
            return self

    def get(self):
        url = f"{self._base_url}/{self._resource_name}/{self.id()}"
        self._logger.debug("get {}", url)
        try:
            r = requests.get(
                url,
                headers=self._headers,
                timeout=self._env.http_timeout(),
            )
            if r.status_code < 200 or r.status_code > 299:
                self._logger.error("failed to get {} {}", self._resource_name, r.text)
            else:
                self._logger.debug("get {} success", self._resource_name)
                self._data = r.json()
        except Exception as e:
            self._logger.error(f"failed to get {self._resource_name} {e}")
        finally:
            return self

    def delete(self):
        url = f"{self._base_url}/{self._resource_name}/{self.id()}"
        self._logger.debug("delete {}", url)
        try:
            r = requests.delete(
                url,
                headers=self._headers,
                timeout=self._env.http_timeout(),
            )
            if r.status_code < 200 or r.status_code > 299:
                self._logger.error(
                    f"failed to delete {self._resource_name} {r.status_code} {r.text}"
                )
            else:
                self._logger.debug(f"delete {self._resource_name} success")
        except Exception as e:
            self._logger.error(f"failed to delete {self._resource_name} {e}")
        finally:
            return self

    def action(self, action_name):
        url = f"{self._base_url}/{self._resource_name}/{self.id()}/{action_name}"
        self._logger.debug("post {}", url)
        try:
            r = requests.post(
                url,
                headers=self._headers,
                timeout=self._env.http_timeout(),
            )
            if r.status_code < 200 or r.status_code > 299:
                self._logger.error(
                    f"failed to post {action_name} on {self._resource_name} {r.text}"
                )
            else:
                self._logger.debug(f"{action_name} {self._resource_name} success")
        except Exception as e:
            self._logger.error(f"failed to {action_name} {self._resource_name} {e}")
        finally:
            return self

    @classmethod
    def list(cls, env=None):
        if env is None:
            env = Env.current()
        headers = env.headers()
        base_url = env.base_url()

        try:
            url = f"{base_url}/{cls._resource_name}/"
            env.logger().debug("get {}", url)
            r = requests.get(url, headers=headers, timeout=env.http_timeout())
            if r.status_code < 200 or r.status_code > 299:
                env.logger().error(f"failed to list {cls._resource_name} {r.text}")
            else:
                env.logger().debug(f"list {cls._resource_name} success")
                result = [cls.build(val, env=env) for val in r.json()]
                return result
        except Exception as e:
            env.logger().error(f"failed to list {cls._resource_name} {e}")
