from datetime import datetime
import os

from tad4bj import *

if __name__ == "__main__":
    ds = DataSchema.load_from_file("./simple_schema.json")
    data = DataStorage("./test.db", "fullcycle", ds)

    jobid = os.getpid()
    h = data.get_handler(jobid)

    h["start"] = datetime.now()
    h["description"] = "This is some text"

    print h["start"]
    print h["flag"]
    assert "flag" not in h

    h["flag"] = 42