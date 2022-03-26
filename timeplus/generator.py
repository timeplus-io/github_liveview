from timeplus.base import Base
from timeplus.source import Source


class GeneratorSource(Source):
    def __init__(self, env=None):
        Source.__init__(self, env)
        self.type("stream_generator")

    def config(self, configuration):
        properties = GeneratorProperties().configuration(configuration)
        self.properties(properties)
        return self


class GeneratorProperties(Base):
    def __init__(self):
        Base.__init__(self)

    def configuration(self, *args):
        return self.prop("configuration", *args)


class GeneratorConfiguration(Base):
    def __init__(self):
        Base.__init__(self)
        self._set("batch_size", 1)
        self._set("interval", 1000)

    def batch(self, *args):
        return self.prop("batch_size", *args)

    def interval(self, *args):
        return self.prop("interval", *args)

    def field(self, field_instance):
        if "fields" not in self._data:
            self._data["fields"] = []
        self._data["fields"].append(field_instance.data())

        return self


class GeneratorField(Base):
    def __init__(self):
        Base.__init__(self)

    def name(self, *args):
        return self.prop("name", *args)

    def type(self, *args):
        return self.prop("type", *args)

    def limit(self, *args):
        return self.prop("limit", *args)

    def timestamp_format(self, *args):
        return self.prop("timestamp_format", *args)
