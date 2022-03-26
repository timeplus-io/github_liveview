import requests

from timeplus.base import Base
from timeplus.resource import ResourceBase


class SourceConnection(Base):
    def __init__(self):
        Base.__init__(self)
        self._set("auto_create", True)

    def stream(self, *args):
        return self.prop("stream_name", *args)

    def auto_create(self, *args):
        return self.prop("auto_create", *args)

    def event_time_column(self, *args):
        return self.prop("event_time_column", *args)


class Source(ResourceBase):
    _resource_name = "sources"

    def __init__(self, env=None):
        ResourceBase.__init__(self, env)

    @classmethod
    def build(cls, id, env=None):
        obj = cls(env=env)
        obj._set("id", id)
        return obj

    def name(self, *args):
        return self.prop("name", *args)

    def connection(self, *args):
        try:
            return self.prop("connection_config", *args)
        except Exception:
            # TODO it is better to change connection_config to connection here
            return self.prop("connection", *args)

    def properties(self, *args):
        return self.prop("properties", *args)

    def type(self, *args):
        return self.prop("type", *args)

    def id(self):
        return self.prop("id")

    def stat(self):
        self.get()
        return self.connection()["stat"]

    def start(self):
        self.action("start")
        return self

    def stop(self):
        self.action("stop")
        return self

    def preview(self, size=3):
        url = f"{self._base_url}/source/preview"
        self._logger.debug("post {}", url)
        previewRequest = {
            "properties": self.properties(),
            "size": size,
            "type": self.type(),
        }

        try:
            r = requests.post(
                url,
                json=previewRequest,
                headers=self._headers,
                timeout=self._env.http_timeout(),
            )
            if r.status_code < 200 or r.status_code > 299:
                self._logger.error(f"failed to preview source {r.status_code} {r.text}")
            else:
                return r.json()
        except Exception as e:
            self._logger.error(f"failed to preivew {e}")
