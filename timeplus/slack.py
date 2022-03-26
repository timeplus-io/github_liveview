from timeplus.sink import Sink
from timeplus.base import Base


class SlackSink(Sink):
    def __init__(self):
        Sink.__init__(self)
        self.type("slack")


class SlackSinkProperty(Base):
    def __init__(self):
        Base.__init__(self)

    def url(self, *args):
        return self.prop("url", *args)

    def message(self, *args):
        return self.prop("template", *args)
