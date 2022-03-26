from timeplus.sink import Sink
from timeplus.base import Base


class SMTPSink(Sink):
    def __init__(self):
        Sink.__init__(self)
        self.type("smtp")


class SMTPSinkProperty(Base):
    def __init__(self):
        Base.__init__(self)

    def f(self, *args):
        return self.prop("from", *args)

    def to(self, *args):
        return self.prop("to", *args)

    def username(self, *args):
        return self.prop("username", *args)

    def host(self, *args):
        return self.prop("host", *args)

    def port(self, *args):
        return self.prop("port", *args)

    def password(self, *args):
        return self.prop("password", *args)

    def message(self, *args):
        return self.prop("message_template", *args)

    def subject(self, *args):
        return self.prop("subject_template", *args)
