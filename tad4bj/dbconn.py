import os
from collections import Mapping, namedtuple
import sqlite3
import json

try:
    import yaml
except ImportError:
    yaml = None

from . import transformers, handlers


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


class DataStorage(Mapping):
    """Abstract the management of all the application data.

    This class will use a SQLite file and present it with more general
    interface to avoid having to cope with SQL and its internals.

    DISCLAIMER: I am using %s string formatting instead of the SQL intended ?
    because there where some issues in certain places (like table name) and
    the user has already access to the database itself, so there's no security
    issue.
    """
    DATABASE_DEFAULT_PATH = os.path.expanduser(os.getenv("TAD4BJ_DATABASE", "~/tad4bj.db"))

    def __init__(self, table_name, path=None):
        """
        :param table_name:
        :param path:
        """
        if path is None:
            path = DataStorage.DATABASE_DEFAULT_PATH

        self._conn = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self._cursor = self._conn.cursor()
        self._table = table_name
        self._metadata = None
        self._rowtuple = None
        self._field_transformers = dict()

    def _get_row_namedtuple(self):
        if self._rowtuple is not None:
            return self._rowtuple

        if not self._cursor.description:
            raise RuntimeError("Could not get row schema --empty cursor")

        self._rowtuple = namedtuple("Row%s" % self._table,
                                    (s[0] for s in self._cursor.description))
        return self._rowtuple

    def close(self):
        if self._conn:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()

    def to_dataframe(self):
        import pandas as pd

        df = pd.read_sql_query("SELECT * FROM %s" % self._table, self._conn)

        return df

    def clear(self, remove_tables=False):
        if remove_tables:
            # Drop them if they exist
            self._cursor.execute("DROP TABLE IF EXISTS `%s_tamd`" % self._table)
            self._cursor.execute("DROP TABLE IF EXISTS `%s`" % self._table)
        else:
            # Application should fail if the table does not exist, as this does:
            self._cursor.execute("DELETE FROM `%s`" % self._table)

    def prepare(self, schema):
        creation_fields = ", ".join("'%s' %s" % (field_name, field_type)
                                    for field_name, field_type in schema.fields)
        self._cursor.execute("CREATE TABLE `%s` (%s)" %
                             (self._table, creation_fields))
        self._metadata = schema.metadata
        self._cursor.execute("CREATE TABLE `%s_tamd` (field text, type text)" %
                             (self._table,))
        self._cursor.executemany("INSERT INTO `%s_tamd` (field, type) values (?, ?)" %
                                 (self._table,), self._metadata.items())
        # Exceptionally, the creation is forcefully committed
        self._conn.commit()

    def update(self, schema):
        self._cursor.execute("SELECT * FROM `%s`" % self._table)
        old_fields = {f[0] for f in self._cursor.description}
        all_fields = {f for f, _ in schema.fields}
        new_fields = all_fields - old_fields

        if not new_fields:
            return

        # Add all new columns
        map(self._cursor.execute,
            ("ALTER TABLE `%s` ADD COLUMN '%s' %s" %
             (self._table, field_name, field_type)
             for field_name, field_type in schema.fields
             if field_name in new_fields))

        self._metadata = schema.metadata

        # Add new elements into the metadata table
        self._cursor.executemany("INSERT INTO `%s_tamd` (field, type) values (?, ?)" %
                                 (self._table,),
                                 ((k, v) for k, v in self._metadata.items()
                                  if k in new_fields))

        # Exceptionally, the alter tables are forcefully committed
        self._conn.commit()

    def _get_field_transformer(self, field_name):
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

    def get_value(self, jobid, field, raw_return=False):
        self._cursor.execute("SELECT `%s` FROM `%s` WHERE id=?" %
                             (field, self._table), (jobid,))
        ret = self._cursor.fetchone()[0]

        if raw_return or ret is None:
            return ret

        # Transform the value before returning it
        return self._get_field_transformer(field).from_db(ret)

    def set_value(self, jobid, field, parameter, raw_parameter=False):
        if raw_parameter:
            value = parameter
        else:
            value = self._get_field_transformer(field).to_db(parameter)

        ex = self._cursor.execute("UPDATE `%s` SET `%s` = ? WHERE id = ?" %
                                  (self._table, field), (value, jobid))
        if ex.rowcount == 0:
            self._cursor.execute("INSERT INTO `%s` (id, `%s`) VALUES (?, ?)" %
                                 (self._table, field), (jobid, value))

    def set_values(self, jobid, fields, parameters, raw_parameters=False):
        set_str = ", ".join("`%s` = ?" % field_name for field_name in fields)

        if raw_parameters:
            values = list(parameters)
        else:
            values = list()

            for field, parameter in zip(fields, parameters):
                values.append(self._get_field_transformer(field).to_db(parameter))

        values.append(jobid)

        ex = self._cursor.execute("UPDATE `%s` SET %s WHERE id = ?" %
                                  (self._table, set_str), values)
        if ex.rowcount == 0:
            fields_str = ", ".join("`%s`" % field_name for field_name in fields)
            question_marks = ", ".join(["?"] * (len(fields) + 1))
            self._cursor.execute("INSERT INTO `%s` (%s, id) VALUES (%s)" %
                                 (self._table, fields_str, question_marks), values)

    def get_handler(self, jobid):
        return handlers.JobHandler(self, jobid)

    def __iter__(self):
        self._cursor.execute("SELECT `id` FROM %s" % (self._table,))
        return (res[0] for res in self._cursor.fetchall())

    def __contains__(self, item):
        self._cursor.execute("SELECT COUNT(*) FROM %s WHERE id = ?" %
                             (self._table,), (item,))
        return self._cursor.fetchone()[0] == 1

    def __getitem__(self, item):
        self._cursor.execute("SELECT * FROM %s WHERE id = ?" %
                             (self._table,), (item,))
        row_raw = self._cursor.fetchone()

        if row_raw is None:
            return KeyError("No row with id=%s" % item)

        row_namedtuple = self._get_row_namedtuple()
        processed_values = list()
        for field, value in zip(row_namedtuple._fields, row_raw):
            if value is not None:
                processed_values.append(
                    self._get_field_transformer(field).from_db(value)
                )
            else:
                processed_values.append(None)

        return row_namedtuple(*processed_values)

    def __len__(self):
        self._cursor.execute("SELECT COUNT(*) FROM %s" %
                             (self._table,))
        return self._cursor.fetchone()[0]


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

    def update(self, schema):
        pass

    def get_value(self, jobid, field, raw_return=False):
        return None

    def set_value(self, jobid, field, value, raw_parameter=False):
        pass

    def set_values(self, jobid, fields, values, raw_parameters=False):
        pass

    def get_handler(self, jobid=1):
        return handlers.JobHandler(self, jobid)
