# Databricks notebook source
# Databricks notebook source
#python imports
import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

# COMMAND ----------

#pyspark imports
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, StructType, StructField, StringType


# COMMAND ----------

# if this notebook is intended to be run on other env. e.g. jenkins agent or other VM,
# spark session need to be explicitely created.
spark = SparkSession.builder.appName('some').enableHiveSupport().getOrCreate()

# COMMAND ----------

# explicitely creating dbutils
# if this notebooks need to be executed through databricks' default python, 
# this function can access dbutils from pyspark.dbutils.
# in other scenarios we can pass dbutils as an argument to our module class

def get_dbutils(self, spark = None):
    try:
        if spark == None:
            spark = spark

        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils
  
dbutils = get_dbutils("", spark = spark)

# COMMAND ----------

#module
class calculator:

	def __init__(self, x = 10, y = 8, dbutils = None, spark = None):
		self.x = x
		self.y = y
		self.dbutils = dbutils
		self.spark = spark
			
	def use_spark(self):
		print(self.spark.range(100).count())
		
	def add(self, x = None, y = None):
		"""add function"""
		if x == None:
			x = self.x
		if y == None:
			y = self.y			
		return x+y

	def subtract(self, x = None, y = None):
		"""subtract function"""
		if x == None:
			x = self.x
		if y == None:
			y = self.y	
		return x-y

	def multiply(self, x = None, y = None):
		"""multiply function"""
		if x == None:
			x = self.x
		if y == None:
			y = self.y			
		return x*y

	def devide(self, x = None, y = None):
		"""devide function"""
		if x == None:
			x = self.x
		if y == None:
			y = self.y			
		if y == 0:
			raise ValueError('cannot devide by zero')
		else:
			return x/y
			
# a provision to execute this script if called from command line just in case, not required in our flow
if __name__ == '__main__':
    inst = calculator(x = 1, y = 2, dbutils = dbutils, spark = spark)
    inst.use_spark()
#     inst.print_dbutils()

# COMMAND ----------


