#!/usr/bin/env python
from __future__ import print_function

import os
import stat
import sys


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("""
First argument should be the path to install the tad4bj entrypoint. E.g.:

  $ ./local_install.py ~/local/bin""")
        exit(1)

    installation_path = sys.argv[1]
    project_path = os.path.dirname(os.path.realpath(__file__))
    script_entrypoint = os.path.join(installation_path, "tad4bj")
    export_line = "export PYTHONPATH=$PYTHONPATH:%s" % project_path

    with open(script_entrypoint, 'w') as f:
        f.write("""#!/bin/sh

PYTHONPATH=%s:$PYTHONPATH python -m tad4bj "$@"
""" % project_path)

    os.chmod(script_entrypoint,
             stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH |
             stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH |
             stat.S_IWUSR)

    with open(os.path.expanduser("~/.bashrc"), 'r') as f:
        if any(s.startswith(export_line)
               for s in f):
            # No more job to do, this is already set
            exit(0)

    # Did not exit, so we need to add the line to .bashrc
    with open(os.path.expanduser("~/.bashrc"), 'a') as f:
        f.writelines([
            "\n",
            "# tad4bj PYTHONPATH setting\n",
            export_line, "\n",
            "###########################\n",
        ])
