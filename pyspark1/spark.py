"""
You can import spark.py into other files to access the SparkSession. Notice that the getOrCreate() method will reuse any SparkSessions that has already been created. Creating a SparkSession is expensive and getOrCreate() will make our test suite run faster because all tests will use the same SparkSession.

The get_spark() function is memoized with the @lru_cache decorator, since the function will be called frequently.

Your production environment will define a specialized SparkSession. For example, the Databricks production environment provides a SparkSession with configuration to run on ec2 clusters. The getOrCreate() method will fetch the SparkSession that is created by Databricks when this code is run in production.
"""

from pyspark.sql import SparkSession
from functools import lru_cache

@lru_cache(maxsize=None)
def get_spark():
    return (SparkSession.builder
                .master("local")
                .appName("gill")
                .getOrCreate())
                
