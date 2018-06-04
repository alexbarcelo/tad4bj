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
        self._data.set_value(self._id, key, value)
        self._inmemory_objects[key] = value

    def __contains__(self, item):
        return self._data.get_value(self._id, item) is not None

    def commit(self):
        self._data.update_values(self._id, self._inmemory_objects)
        self._inmemory_objects.clear()
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
        self._conn = sqlite3.connect(path)
        self._cursor = self._conn.cursor()
        self._table = table_name
        self._metadata = None
        self._field_transformers = dict()

    def close(self):
        self._conn.commit()
        self._conn.close()

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

    def _get_transformer(self, field_name):
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
        data = self._cursor.fetchone()[0]

        t = self._get_transformer(field)
        return t.from_db(data)

    def set_value(self, jobid, field, value):
        t = self._get_transformer(field)
        value = t.to_db(value)

        ex = self._cursor.execute("UPDATE `%s` SET `%s` = ? WHERE id = ?" %
                                  (self._table, field), (value, jobid))
        if ex.rowcount == 0:
            self._cursor.execute("INSERT INTO `%s` (id, `%s`) VALUES (?, ?)" %
                                 (self._table, field), (jobid, value))

    def update_values(self, jobid, value_map):
        values = []
        fields = []

        for field_name, field_value in value_map.items():
            t = self._get_transformer(field_name)
            fields.append(field_name)
            values.append(t.to_db(field_value))

        set_str = ", ".join("`%s` = ?" % field_name for field_name in fields)
        values.append(jobid)
        self._cursor.execute("UPDATE `%s` SET %s WHERE id = ?" %
                             (self._table, set_str), values)

    def get_handler(self, jobid):
        return JobHandler(self, jobid)
