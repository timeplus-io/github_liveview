import requests
import json
from websocket import create_connection
import rx
import dateutil.parser

from timeplus.resource import ResourceBase
from timeplus.env import Env
from timeplus.type import Type
from timeplus.error import TimeplusAPIError


class Query(ResourceBase):
    _resource_name = "queries"

    def __init__(self, env=None):
        ResourceBase.__init__(self, env)
        self.stopped = False

    @classmethod
    def build(cls, query, env=None):
        obj = cls(env=env)
        obj._data = query
        return obj

    @classmethod
    def execSQL(cls, sql, timeout=1000, env=None):
        if env is None:
            env = Env.current()

        headers = env.headers()
        base_url = env.base_url()

        url = f"{base_url}/sql"
        env.logger().debug(f"post {url}")
        sqlRequest = {"sql": sql, "timeout": timeout}

        try:
            r = requests.post(f"{url}", json=sqlRequest, headers=headers)
            if r.status_code < 200 or r.status_code > 299:
                err_msg = f"failed to run sql due to {r.text}"
                raise TimeplusAPIError("post", r.status_code, err_msg)
            else:
                return r.json()
        except Exception as e:
            env.logger.error(f"failed to run sql {e}")
            raise e

    @classmethod
    def exec(cls, sql, env=None):
        if env is None:
            env = Env.current()
        headers = env.headers()
        base_url = env.base_url()

        url = f"{base_url}/exec"
        env.logger.debug(f"post {url}")
        sqlRequest = {
            "sql": sql,
        }

        try:
            r = requests.post(f"{url}", json=sqlRequest, headers=headers)
            if r.status_code < 200 or r.status_code > 299:
                err_msg = f"failed to run exec due to {r.text}"
                raise TimeplusAPIError("post", r.status_code, err_msg)
            else:
                return r.json()
        except Exception as e:
            env.logger.error(f"failed to run exec {e}")
            raise e

    def name(self, *args):
        return self.prop("name", *args)

    def description(self, *args):
        return self.prop("description", *args)

    def sql(self, *args):
        return self.prop("sql", *args)

    def tags(self, *args):
        return self.prop("tags", *args)

    def id(self):
        return self.prop("id")

    def stat(self):
        self.get()
        return self.prop("stat")

    def status(self):
        self.get()
        return self.prop("status")

    def header(self):
        self.get()
        return self.prop("result")["header"]

    def cancel(self):
        self.action("cancel")
        return self

    def stop(self):
        self.stopped = True

    def sink_to(self, sink):
        url = f"{self._base_url}/{self._resource_name}/{self.id()}/sinks"
        self._logger.debug(f"post {url}")
        sinkRequest = {"sink_id": sink.id()}
        try:
            r = requests.post(f"{url}", json=sinkRequest, headers=self._headers)
            if r.status_code < 200 or r.status_code > 299:
                err_msg = f"failed to add sink {sink.id()} to query {self.id()} {r.status_code} {r.text}"
                raise TimeplusAPIError("post", r.status_code, err_msg)
            else:
                self._logger.debug(f"add sink {sink.id()} to query {self.id()} success")
                return self
        except Exception as e:
            self._logger.error(f"failed to add sink {e}")
            raise e

    def show_query_result(self, count=10):
        ws_schema = "ws"
        if self._env.schema() == "https":
            ws_schema = "wss"
        ws = create_connection(
            f"{ws_schema}://{self._env.host()}:{self._env.port()}/ws/queries/{self.id()}"
        )
        for i in range(count):
            result = ws.recv()
            self._logger.info(result)

    # TODO: refactor this complex method
    def _query_op(self):  # noqa: C901
        def __query_op(observer, scheduler):
            # TODO : use WebSocketApp
            ws_schema = "ws"
            if self._env.schema() == "https":
                ws_schema = "wss"
            ws = create_connection(
                f"{ws_schema}://{self._env.host()}:{self._env.port()}/ws/queries/{self.id()}"
            )
            try:
                while True:
                    if self.stopped:
                        break
                    result = ws.recv()
                    # convert string object to json(array)
                    # todo convert by header type
                    record = json.loads(result)

                    for index, col in enumerate(self.header()):
                        if col["type"].startswith(Type.Tuple.value):
                            record[index] = tuple(record[index])
                        elif col["type"].startswith(Type.Date.value):
                            try:
                                record[index] = dateutil.parser.isoparse(record[index])
                            except Exception as e:
                                self._logger.error("failed to parse datetime ", e)

                    observer.on_next(record)
                observer.on_complete()
            except Exception:
                pass

        return __query_op

    def get_result_stream(self):
        strem_query_ob = rx.create(self._query_op())
        return strem_query_ob
