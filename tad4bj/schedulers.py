import os

from .dbconn import DataStorage, DummyDataStorage

class Slurm:
    JOB_ID = "SLURM_JOB_ID"
    TABLE_NAME = "SLURM_JOB_NAME"


class Pbs:
    JOB_ID = "PBS_JOBID"
    TABLE_NAME = "PBS_JOBNAME"


known_schedulers = [Slurm, Pbs]


def prepare_handler(scheduler_environ_vars):
    return DataStorage(
        os.environ[scheduler_environ_vars.TABLE_NAME]
    ).get_handler(int(os.environ[scheduler_environ_vars.JOB_ID]))


def prepare_dummy_handler():
    return DummyDataStorage().get_handler()


def auto_detect_table_name():
    for sch in known_schedulers:
        try:
            return os.environ[sch.TABLE_NAME]
        except KeyError:
            pass
    raise ValueError("Could not autodetect JOB_NAME (table name)")


def auto_detect_job_id():
    for sch in known_schedulers:
        try:
            return int(os.environ[sch.JOB_ID])
        except KeyError:
            pass
    raise ValueError("Could not autodetect JOB_ID (row identifier)")
