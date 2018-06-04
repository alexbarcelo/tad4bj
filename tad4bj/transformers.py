import json
try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import yaml
except ImportError:
    yaml = None


class Identity(object):
    @staticmethod
    def from_db(data):
        return data

    @staticmethod
    def to_db(data):
        return data


class JsonTransformer(object):
    @staticmethod
    def from_db(data):
        return json.loads(bytes(data))

    @staticmethod
    def to_db(data):
        return json.dumps(data)


class YamlTransformer(object):
    @staticmethod
    def from_db(data):
        if yaml is None:
            raise ImportError("No YAML library available, YAML is unsupported")
        return yaml.load(bytes(data))

    @staticmethod
    def to_db(data):
        if yaml is None:
            raise ImportError("No YAML library available, YAML is unsupported")
        return yaml.dump(data)


class PickleTransformer(object):
    @staticmethod
    def from_db(data):
        return pickle.loads(bytes(data))

    @staticmethod
    def to_db(data):
        return pickle.dumps(data)
