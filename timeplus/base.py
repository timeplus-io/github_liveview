class Base:
    def __init__(self):
        self._data = {}

    def prop(self, name, *args):
        if len(args) == 0:
            return self._get(name)
        elif len(args) == 1:
            return self._set(name, args[0])
        else:
            raise Exception("invalid number of arguments")

    def _set(self, key, value):
        if isinstance(value, Base):
            self._data[key] = value.data()
        else:
            self._data[key] = value
        return self

    def _get(self, key):
        return self._data[key]

    def data(self):
        return self._data

    def id(self):
        return self._get("id")
