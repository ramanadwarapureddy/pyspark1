"""
Let’s add a mission.py file with a DataFrame transformation that appends a life_goal column to a DataFrame.

gill/
  gill/
    __init__.py
    mission.py
    spark.py
  .python-version

"""
import pyspark.sql.functions as F
    
def with_life_goal(df):
    return df.withColumn("life_goal", F.lit("escape!"))



"""
Consistent with PySpark best practices, we’re importing the PySpark SQL functions as F.
The DataFrame.withColumn method is used to append a column to a DataFrame.
"""
