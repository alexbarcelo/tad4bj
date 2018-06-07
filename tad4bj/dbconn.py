import sqlite3
import json

try:
    from numbers import Number
except ImportError:
    Number = (int, long, float, complex)

try:
    import yaml
except ImportError:
    yaml = None

try:
    Text = basestring
except NameError:
    Text = (str, bytes)

from . import transformers


class DataSchema(object):
    def __init__(self, dict):
        # ToDo: some error and sanity checking
        self.__dict__.update(dict)

    @classmethod
    def load_from_file(cls, path):
        """

        :param path:
        """
        with open(path, "r") as f:
            if path.endswith(".json"):
                dict_data = json.load(f)
            elif path.endswith(".yaml"):
                if not yaml:
                    raise ImportError("No YAML available --could not load %s file" % path)
                dict_data = yaml.load(f)
            else:
                raise NotImplementedError("File type not recognized, unable to read %s" % path)

        return cls(dict_data)


class JobHandler(object):
    """

    """
    def __init__(self, datastorage, jobid):
        self._id = jobid
        self._data = datastorage
        self._inmemory_objects = dict()

    def __getitem__(self, item):
        if not isinstance(item, Text):
            raise ValueError("Field names must be strings")

        try:
            return self._inmemory_objects[item]
        except KeyError:
            pass

        raw_data = self._data.get_value(self._id, item)
        if raw_data is None:
            raise KeyError("Field %s is NULL" % item)

        data = self._data.get_field_transformer(item).from_db(raw_data)
        self._inmemory_objects[item] = data
        return data

    def __setitem__(self, key, value):
        if not isinstance(key, Text):
            raise ValueError("Field names must be strings")
        tf = self._data.get_field_transformer(key)
        raw_data = tf.to_db(value)

        self._data.set_value(self._id, key, raw_data)
        self._inmemory_objects[key] = value

    def __contains__(self, item):
        if not isinstance(item, Text):
            raise ValueError("Field names must be strings")

        try:
            value = self._inmemory_objects[item]
            return value is not None
        except KeyError:
            # No need to transform, just check if it is set
            return self._data.get_value(self._id, item) is not None

    def get(self, item, default=None):
        if not isinstance(item, Text):
            raise ValueError("Field names must be strings")

        # Starts just like __getitem__
        try:
            return self._inmemory_objects[item]
        except KeyError:
            pass

        raw_data = self._data.get_value(self._id, item)

        if raw_data is not None:
            ret = self._data.get_field_transformer(item).from_db(raw_data)
        else:
            # provided default needs not to be transformed
            ret = default

        return ret

    def setdefault(self, item, default=None):
        # does a get, and stores it **if** we are not receiving None
        ret = self.get(item, default)
        if ret is not None:
            self._inmemory_objects[item] = ret
        return ret

    def commit(self):
        fields = []
        values = []
        for field_name, field_value in self._inmemory_objects.items():
            t = self._data.get_field_transformer(field_name)
            fields.append(field_name)
            values.append(t.to_db(field_value))

        self._data.set_values(self._id, fields, values)
        self._data._conn.commit()

    def __del__(self):
        self.commit()


class DataStorage(object):
    """Abstract the management of all the application data.

    This class will use a SQLite file and present it with more general
    interface to avoid having to cope with SQL and its internals.

    DISCLAIMER: I am using %s string formatting instead of the SQL intended ?
    because there where some issues in certain places (like table name) and
    the user has already access to the database itself, so there's no security
    issue.
    """
    def __init__(self, path, table_name):
        """

        :param path:
        :param table_name:
        :param schema:
        :param kwargs:
        """
        self._conn = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self._cursor = self._conn.cursor()
        self._table = table_name
        self._metadata = None
        self._field_transformers = dict()

    def close(self):
        if self._conn:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()

    def clear(self, remove_tables=False):
        if remove_tables:
            try:
                self._cursor.execute("DROP TABLE `%s_tamd`" % self._table)
            except sqlite3.OperationalError:
                pass
            try:
                self._cursor.execute("DROP TABLE `%s`" % self._table)
            except sqlite3.OperationalError:
                pass
        else:
            self._cursor.execute("DELETE FROM `%s`" % self._table)

    def prepare(self, schema):
        creation_fields = ", ".join("'%s' %s" % (field_name, field_type)
                                    for field_name, field_type in schema.fields)
        self._cursor.execute("CREATE TABLE `%s` (%s)" %
                             (self._table, creation_fields))
        self._metadata = schema.metadata
        self._cursor.execute("CREATE TABLE `%s_tamd` (field text, type text)" %
                             (self._table,))
        # Exceptionally, the creation is forcefully committed
        self._conn.commit()
        self._cursor.executemany("INSERT INTO `%s_tamd` (field, type) values (?, ?)" %
                                 (self._table,), self._metadata.items())

    def get_field_transformer(self, field_name):
        try:
            # Fast scenario: the transformer is cached in its place
            return self._field_transformers[field_name]
        except KeyError:
            pass
        if self._metadata is None:
            self._cursor.execute("SELECT * FROM `%s_tamd`" % (self._table,))
            result = self._cursor.fetchall()
            self._metadata = {k: v for k, v in result}

        field_type = self._metadata.get(field_name)
        if field_type is None:
            tf = transformers.Identity
        elif field_type == "pickle":
            tf = transformers.PickleTransformer
        elif field_type == "json":
            tf = transformers.JsonTransformer
        elif field_type == "yaml":
            tf = transformers.YamlTransformer
        else:
            raise ValueError("Unknown field_type for TAD4BJ: %s" % field_type)

        self._field_transformers[field_name] = tf
        return tf

    def get_value(self, jobid, field):
        self._cursor.execute("SELECT `%s` FROM `%s` WHERE id=?" %
                             (field, self._table), (jobid,))
        return self._cursor.fetchone()[0]

    def set_value(self, jobid, field, value):
        ex = self._cursor.execute("UPDATE `%s` SET `%s` = ? WHERE id = ?" %
                                  (self._table, field), (value, jobid))
        if ex.rowcount == 0:
            self._cursor.execute("INSERT INTO `%s` (id, `%s`) VALUES (?, ?)" %
                                 (self._table, field), (jobid, value))

    def set_values(self, jobid, fields, values):
        set_str = ", ".join("`%s` = ?" % field_name for field_name in fields)
        values = list(values) + [jobid]

        ex = self._cursor.execute("UPDATE `%s` SET %s WHERE id = ?" %
                                  (self._table, set_str), values)
        if ex.rowcount == 0:
            fields_str = ", ".join("`%s`" % field_name for field_name in fields)
            question_marks = ", ".join(["?"] * (len(fields) + 1))
            self._cursor.execute("INSERT INTO `%s` (%s, id) VALUES (%s)" %
                                 (self._table, fields_str, question_marks), values)

    def get_handler(self, jobid):
        return JobHandler(self, jobid)


class DummyDataStorage(object):
    """Seems  DataStorage, but does nothing and doesn't raise expcetions (almost)."""

    def __init__(self, *args, **kwargs):
        pass

    def close(self):
        pass

    def clear(self, remove_tables=False):
        pass

    def prepare(self, schema):
        raise NotImplementedError("Refusing to dummy-prepare a table. I am a dummy.")

    def get_field_transformer(self, field_name):
        return transformers.Identity

    def get_value(self, jobid, field):
        return None

    def set_value(self, jobid, field, value):
        pass

    def set_values(self, jobid, fields, values):
        pass

    def get_handler(self, jobid=1):
        return JobHandler(self, jobid)
