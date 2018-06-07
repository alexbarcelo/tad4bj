try:
    Text = basestring
except NameError:
    Text = (str, bytes)


class JobHandler(object):
    """

    """
    def __init__(self, datastorage, jobid):
        self._id = jobid
        self._data = datastorage
        self._inmemory_objects = dict()

    def __delitem__(self, key):
        if not isinstance(key, Text):
            raise ValueError("Field names must be strings")
        self._data.set_value(self._id, key, None, raw_parameter=True)

    def __getitem__(self, item):
        if not isinstance(item, Text):
            raise ValueError("Field names must be strings")

        try:
            return self._inmemory_objects[item]
        except KeyError:
            pass

        data = self._data.get_value(self._id, item)
        if data is None:
            raise KeyError("Field %s is NULL" % item)

        self._inmemory_objects[item] = data
        return data

    def __setitem__(self, key, value):
        if not isinstance(key, Text):
            raise ValueError("Field names must be strings")

        self._data.set_value(self._id, key, value)
        self._inmemory_objects[key] = value

    def __contains__(self, item):
        if not isinstance(item, Text):
            raise ValueError("Field names must be strings")

        try:
            value = self._inmemory_objects[item]
            return value is not None
        except KeyError:
            # No need to transform, just check if it is set
            return self._data.get_value(self._id, item, raw_return=True) is not None

    def get(self, item, default=None):
        if not isinstance(item, Text):
            raise ValueError("Field names must be strings")

        # Starts just like __getitem__
        try:
            return self._inmemory_objects[item]
        except KeyError:
            pass

        ret = self._data.get_value(self._id, item)

        if ret is None:
            return default
        else:
            return ret

    def setdefault(self, item, default=None):
        # does a get, and stores it **if** we are not receiving None
        ret = self.get(item, default)
        if ret is not None:
            self._inmemory_objects[item] = ret
        return ret

    def commit(self):
        self._data.set_values(self._id,
                              self._inmemory_objects.keys(),
                              self._inmemory_objects.values())
        self._data._conn.commit()

    def __del__(self):
        self.commit()
