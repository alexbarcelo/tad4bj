import sqlite3
import json

try:
    import yaml
except ImportError:
    yaml = None


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

    def __getitem__(self, item):
        return self._data.get_value(self._id, item)

    def __setitem__(self, key, value):
        self._data.set_value(self._id, key, value)

    def __contains__(self, item):
        return self._data.get_value(self._id, item) is not None


class DataStorage(object):
    """Abstract the management of all the application data.

    This class will use a SQLite file and present it with more general
    interface to avoid having to cope with SQL and its internals.

    DISCLAIMER: I am using %s string formatting instead of the SQL intended ?
    because there where some issues in certain places (like table name) and
    the user has already access to the database itself, so there's no security
    issue.
    """
    def __init__(self, path, table_name, schema):
        """

        :param path:
        :param table_name:
        :param schema:
        :param kwargs:
        """
        self._conn = sqlite3.connect(path)
        self._cursor = self._conn.cursor()
        self._table = table_name
        self._schema = schema
        self._prepare()

    def close(self):
        self._conn.commit()
        self._conn.close()

    def __del__(self):
        self.close()

    def clear(self):
        self._cursor.execute("DROP TABLE '%s'" % self._table)

    def _prepare(self):
        creation_fields = ", ".join("'%s' %s" % (field_name, field_type)
                                    for field_name, field_type in self._schema.fields)
        self._cursor.execute("CREATE TABLE IF NOT EXISTS '%s' (%s)" %
                             (self._table, creation_fields))

    def get_value(self, jobid, field):
        self._cursor.execute("SELECT `%s` FROM `%s` WHERE id=?" %
                             (field, self._table), (jobid,))
        return self._cursor.fetchone()[0]

    def get_values(self, jobid):
        self._cursor.execute("SELECT * FROM '%s' WHERE id=?" %
                             (self._table,), (jobid,))
        return self._cursor.fetchone()

    def set_value(self, jobid, field, value):
        ex = self._cursor.execute("UPDATE `%s` SET `%s` = ? WHERE id = ?" %
                                  (self._table, field), (value, jobid))
        if ex.rowcount == 0:
            self._cursor.execute("INSERT INTO `%s` (id, `%s`) VALUES (?, ?)" %
                                 (self._table, field), (jobid, value))

    def get_handler(self, jobid):
        return JobHandler(self, jobid)
