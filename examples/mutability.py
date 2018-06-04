from __future__ import print_function
from datetime import datetime
import os

from tad4bj import *

if __name__ == "__main__":
    ds = DataSchema.load_from_file("./simple_schema.json")
    data = DataStorage("./test.db", "main_test")

    data.clear(remove_tables=True)
    data.prepare(ds)

    jobid = os.getpid()
    h = data.get_handler(jobid)

    h["start"] = datetime.now()
    h["pickled_item"] = [1, (2, "two"), 3, {4: "four", 5: "five"}]
    mutable_stuff = [6, 7, 8, 9]
    h["yaml_item"] = mutable_stuff

    data = h["pickled_item"]
    data[3][6] = "six"
    data.append(7)

    mutable_stuff.extend([10, 11, 12, 13, 14, 15])

    print(h["yaml_item"])
    h.commit()
    print(h["pickled_item"])
    print(h["yaml_item"])
