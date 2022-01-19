# Databricks notebook source
# MAGIC %run ./MyUDF

# COMMAND ----------

import unittest
import coverage
import xmlrunner

class MyNotebookTests(unittest.TestCase):
    def test_simple_repeat_word(self):
        self.assertEqual(len(create_sample_df().columns), 2)
        self.assertNotEqual(len(create_sample_df().columns), 10)

cov = coverage.Coverage("/tmp/.coverage")
cov.start()

suite =  unittest.TestLoader().loadTestsFromTestCase(MyNotebookTests)
runner = xmlrunner.XMLTestRunner(output='/tmp/testreport.xml')
runner.run(suite)

cov.stop()
print(cov)
cov.save()
cov.html_report(directory='/tmp/covhtml')
# cov = coverage.Coverage()
# cov.start()

# suite = unittest.TestLoader().loadTestsFromTestCase(MyNotebookTests)
# runner = unittest.TextTestRunner(verbosity=2)
# runner.run(suite)

# cov.stop()
# cov.save()
# cov.html_report(directory='/dbfs/covhtml')

# COMMAND ----------

# from unittest import TestCase
# from pyspark.sql.types import StringType

# # @pytest.fixture(scope='function', autouse=True)
# def mock_udf_annotation(monkeypatch):
#     def dummy_udf(f):
#         return f

#     def mock_udf(f=None, returnType=StringType()):
#         return f if f else dummy_udf

#     monkeypatch.setattr('pyspark.sql.functions.udf', mock_udf)

# class TestUDFs(TestCase):

#     def test_upper(self):
#         """
#         @udf(returnType=ArrayType(StringType()))
#         def to_upper_list(s):
#             return [i.upper() for i in s]
#         """
#         from our_package.spark import udfs as UDF
#         self.assertEqual(UDF.to_upper_list(['potato', 'carrot', 'tomato']), ['POTATO', 'CARROT', 'TOMATO'])
        
# #TODO check sparkContext is runLocal
