from timeplus.resource import ResourceBase


class Sink(ResourceBase):
    _resource_name = "sinks"

    def __init__(self, env=None):
        ResourceBase.__init__(self, env)

    @classmethod
    def build(cls, id, env=None):
        obj = cls(env=env)
        obj._set("id", id)
        return obj

    def name(self, *args):
        return self.prop("name", *args)

    def id(self):
        return self.prop("id")

    def type(self, *args):
        return self.prop("type", *args)

    def properties(self, *args):
        return self.prop("properties", *args)
