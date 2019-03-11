# pyspark1
This project will deals with the pyspark project structure.


Structuring PySpark Jobs

Handling 3rd-party dependencies

Writing a PySpark Job

Unit Testing

Structuring our Jobs Repository

First, let’s go over how submitting a job to PySpark works:
spark-submit --py-files pyfile.py,zipfile.zip main.py --arg1 val1

When we submit a job to PySpark we submit the main Python file to run — main.py — and we can also add a list of dependent files that will be located together with our main file during execution.
These dependency files can be .py code files we can import from, but can also be any other kind of files. For example, .zip packages.

One of the cool features in Python is that it can treat a zip file as a directory as import modules and functions from just as any other directory. 
All that is needed is to add the zip file to its search path.

import sys
sys.path.insert(0, jobs.zip)
now (assuming jobs.zip contains a python module called jobs) we can import that module and whatever that’s in it. For example:

from jobs.wordcount import run_job
run_job()
This will allow us to build our PySpark job like we’d build any Python project — using multiple modules and files — rather than one bigass myjob.py (or several such files)

Armed with this knowledge let’s structure out PySpark project…

Jobs as Modules
We’ll define each job as a Python module where it can define its code and transformation in whatever way it likes (multiple files, multiple sub modules…).

.
├── README.md
├── src
│   ├── main.py
│   ├── jobs
│   │   └── wordcount
│   │       └── __init__.py

The job itself has to expose an analyze function:

def analyze(sc, **kwargs):
   ...
and a main.py which is the entry point to our job — it parses command line arguments and dynamically loads the requested job module and runs it:

import pyspark
if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')
parser = argparse.ArgumentParser()
parser.add_argument('--job', type=str, required=True)
parser.add_argument('--job-args', nargs='*')
args = parser.parse_args()
sc = pyspark.SparkContext(appName=args.job_name)
job_module = importlib.import_module('jobs.%s' % args.job)
job_module.analyze(sc, job_args)
To run this job on Spark we’ll need to package it so we can submit it via spark-submit …

Packaging
As we previously showed, when we submit the job to Spark we want to submit main.py as our job file and the rest of the code as a --py-files extra dependency jobs.zipfile.
So, out packaging script (we’ll add it as a command to our Makefile) is:

build:
    mkdir ./dist
    cp ./src/main.py ./dist
    cd ./src && zip -x main.py -r ../dist/jobs.zip .
Now we can submit our job to Spark:

make build
cd dist && spark-submit --py-files jobs.zip main.py --job wordcount
If you noticed before, out main.py code runs 
sys.path.insert(0, 'jobs.zip)
making all the modules inside it available for import.
Right now we only have one such module we need to import — jobs — which contains our job logic.

We can also add a shared module for writing logic that is used by multiple jobs. That module we’ll simply get zipped into jobs.zip too and become available for import.

.
├── Makefile

├── README.md

├── src

│   ├── main.py

│   ├── jobs

│   │   └── wordcount

│   │       └── __init__.py

│   └── shared

│       └── __init__.py

Handling 3rd Party Dependencies
One of the requirements anyone who’s writing a job bigger the the “hello world” probably needs to depend on some external python pip packages.

To use external libraries, we’ll simply have to pack their code and ship it to spark the same way we pack and ship our jobs code. 
pip allows installing dependencies into a folder using its -t ./some_folder options.

The same way we defined the shared module we can simply install all our dependencies into the src folder and they’ll be packages and be available for import the same way our jobs and shared modules are:

pip install -r requirements.txt -t ./src
However, this will create an ugly folder structure where all our requirement’s code will sit in source, overshadowing the 2 modules we really care about: shared and jobs

That’s why I find it useful to add a special folder — libs — where I install requirements to:

.
├── Makefile
├── README.md
├── requirements.txt
├── src
│   ├── main.py
│   ├── jobs
│   │   └── wordcount
│   │       └── __init__.py
│   └── libs
│   │   └── requests
│   │   └── ...
│   └── shared
│       └── __init__.py
With our current packaging system will break imports as import some_package will now have to be written as import libs.some_package.
To solve that we’ll simply package our libs folder into a separate zip package who’s root older is libs.

build: clean
 mkdir ./dist
 cp ./src/main.py ./dist
 cd ./src && zip -x main.py -x \*libs\* -r ../dist/jobs.zip .
 cd ./src/libs && zip -r ../../dist/libs.zip .
Now we can import our 3rd party dependencies without a libs. prefix, and run our job on PySpark using:

cd dist
spark-submit --py-files jobs.zip,libs.zip main.py --job wordcount
The only caveat with this approach is that it can only work for pure-Python dependencies. For libraries that require C++ compilation, there’s no other choice but to make sure they’re pre-installed on all nodes before the job runs which is a bit harder to manage. Fortunately, most libraries do not require compilation which makes most dependencies easy to manage,

Writing a PySpark Job
The next section is how to write a jobs’s code so that it’s nice, tidy and easy to test.

Providing a Shared Context
When writing a job, there’s usually some sort of global context we want to make available to the different transformation functions. 
Spark broadcast variables, counters, and misc configuration data coming from command-line are the common examples for such job context data.

For this case we’ll define a JobContext class that handles all our broadcast variables and counters:

from collections import OrderedDict
from tabulate import tabulate
class JobContext(object):
  def __init__(self, sc):
    self.counters = OrderedDict()
    self._init_accumulators(sc)
    self._init_shared_data(sc)
  def _init_accumulators(self, sc):
    pass
  def _init_shared_data(self, sc):
    pass
  def initalize_counter(self, sc, name):
    self.counters[name] = sc.accumulator(0)
  def inc_counter(self, name, value=1):
    if name not in self.counters:
      raise ValueError("%s counter was not initialized. (%s)" % (name, self.counters.keys()))
    self.counters[name] += value
  def print_accumulators(self):
    print 'aa\n' * 2
    print tabulate(self.counters.items(), 
                   self.counters.keys(), 
                   tablefmt="simple")
We’ll create an instance of it on our job’s code and pass it to our transformations.
For example, let’s say we want to test the number of words on our wordcount job:

class WordCountJobContext(JobContext):
  def _init_accumulators(self, sc):
    self.initalize_counter(sc, 'words')
def to_pairs(context, word):
  context.inc_counter('words')
  return word, 1
def analyze(sc):
  print "Running wordcount"
  context = WordCountJobContext(sc)
  text = " ...  some text ..."
  words = sc.parallelize(text.split())
  pairs = words.map(lambda word: to_pairs(context, word))
  ordered = counts.sortBy(lambda pair: pair[1], ascending=False)
  print ordered.collect()
  context.print_accumulators()
Besides sorting the words by occurrence, we’ll now also keep a distributed counter on our context that counts the number of words we processed in total. We can then nicely print it at the end by calling `context.print_accumulators()` or access it via context.counters['words']

Writing Transformations
The code above is pretty cumbersome to write instead of simple transformations that look like pairs = words.map(to_pairs) we now have this extra context parameter requiring us to write a lambda expression: pairs = words.map(lambda word: to_pairs(context, word)

So we’ll use functools.partial to make our code nicer:

def analyze(sc):
  print "Running wordcount"
  context = WordCountJobContext(sc)
  text = " ...  some text ..."
  to_pairs_step = partial(to_pairs, context)
  words = sc.parallelize(text.split())
  pairs = words.map(to_pairs_step)
  ordered = counts.sortBy(lambda pair: pair[1], ascending=False)
  print ordered.collect()
  context.print_accumulators()
Unit Testing
When looking at PySpark ode, there are few ways we can (should) test our code:

Transformation Tests — since transformations (like our to_pairs above) are just regular Python functions, we can simply test them the same way we’d test any other python Function

from mock import MagicMock
from jobs.wordcount import to_pairs
def test_to_pairs():
  context_mock = MagicMock()
  result = to_pairs(context_mock, 'foo')
  assert result[0] == 'foo'
  assert result[1] == 1
  context_mock.inc_counter.assert_called_with('words')
These tests cover 99% of our code, so if we just test our transformations we’re mostly covered.

Entire Flow Tests — testing the entire PySpark flow is a bit tricky because Spark runs in JAVA and as a separate process.
The best way to test the flow is to fake the spark functionality.
The PySparking is a pure-Python implementation of the PySpark RDD interface. 
It acts like a real Spark cluster would, but implemented Python so we can simple send our job’s analyze function a pysparking.Contextinstead of the real SparkContext to make our job run the same way it would run in Spark.
Since we’re running on pure Python we can easily mock things like external http requests, DB access etc. which is necessary for writing good unit tests.

import pysparkling
from mock import patch
from jobs.wordcount import analyze
@patch('jobs.wordcount.get_text')
def test_wordcount(get_text_mock):
  get_text_mock.return_value = "foo bar foo"
  sc = pysparkling.Context()
  result = analyze(sc)
  assert result[0] == ('foo', 2)
  assert result[1] == ('bar', 1)
Testing the entire job flow requires refactoring the job’s code a bit so that analyze returns a value to be tested and that the input is configurable so that we could mock it.
