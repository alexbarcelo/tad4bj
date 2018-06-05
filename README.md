# Tabular Annotations of Data for Batch Jobs

Do you use a queue system (slurm, pbs) and are tired of manually tracking result of your executions?

So was I. So I decided to start this small project. This project is suited for you if you...

 - ... execute the same application with different parameters
 - ... want to track the execution parameters and/or its outputs
 - ... the data of interest can easily be displayed in a tabular fashion
 - ... don't want to track the data yourself by going into the log files generated in the application

**You don't need to install any additional dependency or package**. You will only need the system python interpreter (either Python 2.7 or 3.x will work). YAML is supported if you desire to use it, but it is entirely optional. The main design decision is to be cluster-friendly, and those environments can be very restricted, but I have always found Python 2.7 in them by default. Hopefully, `tad4bj` will keep working regardless of being inside a virtual environment or changing versions.

The application `tad4bj` can be used both through the native python bindings or through the shell. There are extra features if you use it through its bindings --e.g. arbitrary pickled structures and mutable types.

Internally, the data is stored into a SQLite database. You can browse it yourself, given that the "schema overhead" is kept at its minimum.

## Quickstart

Keep reading for quick examples and steps to have `tad4bj` working for you in your environment.

### Installation

 1. Download and extract the zip file into your cluster's home (or execution environment).
 2. Execute the `./local_install.py ~/bin` and don't use _sudo_! If you have your user scripts elsewhere, change the path.
 3. Logout and login.
 
### Basic shell usage and examples

Command line help is more or less useful:

`tad4bj -h`

First of all you will need a schema for your table. Look into the examples folder or go to [Schema](#schema) for more detailed documentation. Then you can prepare the database with:

`tad4bj --table <mytablename> init <schemafile.json>`

A file will have just appeared in your home called `tad4bj.db`. You can tune that if you don't like this path. This is a SQLite database, so feel free to browse it or extract information from there.

You can use it `tad4bj` cli, e.g.:

```
$ tad4bj --table <mytablename> set --jobid 123 description "Hey look this is a string description of this"
$ tad4bj --table <mytablename> get --jobid 123 description
Hey look this is a string description of this
$ tad4bj --table <mytablename> set --jobid 124 description "Another execution, another description"
$ tad4bj --table <mytablename> setdict --jobid 123 '{"description": "this also works", "flag": 42}'
$ tad4bj --table <mytablename> setdict --jobid 124 --dialect yaml - < a_dict.yaml
```

### Using python bindings

Assuming that you are using either slurm or pbs and the python code is being executed inside a submitted job, then it's easy! Just use it like this:

```python
from tad4bj.slurm import handler as tadh
from datetime import datetime

...

tadh["start"] = datetime.now()

elements = [1, 2, 3]

tadh["pickled_item"] = elements

elements.append(4)  # note that pickled_item column will be (eventually) updated!

# and you can also get data
descr = tadh["description"]
```

Some relevant notes regarding the python bindings:

 - The import is working because `PYTHONPATH` is updated in your `.bashrc` (see [Installation](#installation))
 - Mutable types have certain quirks, see [Mutable types](#mutable-types) for some additional notes

### What happens with `--table` and `--jobid`

The previous examples have explicit table name and job identifier. By default, if you are using slurm or pbs, those parameters are not needed:

 - The job name will be used as table name
 - The job identifier will be used as row identifier --aka `--jobid`

Those values from the scheduler are taken from their default environment variables. You can change the behaviour simply by keep using explicit `--table` and `--jobid` flags. Remember while creating the table (`tad4bj init`) to match the job name that you are using --or the other way around, use the same job name to ensure that the annotations are correctly stored in the created table. Otherwise you will have SQL errors of table not exists.

By using the job scheduler autodetection, using `tad4bj` from inside submitted jobs is easy:

```
tad4bj set description "Submitted jobs are easy to annotate"
```

## Schema

**ToDo**

## Mutable types

While using the python bindings, you can use structured fields which can contain mutable types --e.g. a JSON field with a lists. The main design decision is that the binding tracks the objects that have been assigned to (or read from) the database.

When the application ends (clean shutdown), all objects in memory are written. That means that if a mutable object has been assigned to the database and then modified, the updated version will be written to the database.

If there is a dirty shutdown (for instance, a job scheduler time limit) the database may not be updated or even some assignments may be lost. You may want to manually call to the `commit` method in the handler to ensure that the database is updated:

```python
tadh.commit()
```

This ensures that all the mutable objects are updated and the database file is updated. It is a good idea to call this `commit` method before time-consuming or crash-prone blocks. 
