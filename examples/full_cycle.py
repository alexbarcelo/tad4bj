from __future__ import print_function
from datetime import datetime
import os

from tad4bj import *

if __name__ == "__main__":
    ds = DataSchema.load_from_file("./simple_schema.json")
    data = DataStorage("./test.db", "main_test")

    data.clear(remove_metadata=True)
    data.prepare(ds)

    jobid = os.getpid()
    h = data.get_handler(jobid)

    h["start"] = datetime.now()
    h["description"] = "This is some text"

    print(h["start"])
    assert "flag" not in h

    h["flag"] = 42

    h["pickled_item"] = (1, [2, "two"], 3, {4, 5, 6}, {7: "8"})
    h["json_item"] = (1, [2, "two"], 3, [4, 5, 6], {7: "8"})
    h["yaml_item"] = (1, [2, "two"], 3, {4, 5, 6}, {7: "8"})

    print(h["pickled_item"])
    print(h["json_item"])
    print(h["yaml_item"])
