import os

os.environ["SLURM_JOB_ID"] = str(os.getpid())
os.environ["SLURM_JOB_NAME"] = "main_test"

os.environ["TAD4BJ_DATABASE"] = "./test.db"

from tad4bj.slurm import handler as tadh
from datetime import datetime


if __name__ == "__main__":
    tadh["start"] = datetime.now()
