# Databricks notebook source
import unittest
import pyspark


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[*]").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

# COMMAND ----------

# Databricks notebook source
import sys
import xmlrunner
import coverage

class SimpleTestCase(PySparkTestCase):

    def test_with_rdd(self):
        test_input = [
            ' hello spark ',
            ' hello again spark spark'
        ]

        input_rdd = self.sc.parallelize(test_input, 1)

        from operator import add

        results = input_rdd.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(add).collect()
        self.assertEqual(results, [('hello', 2), ('spark', 3), ('again', 1)])

    def test_with_df(self):
        df = self.spark.createDataFrame(data=[[1, 'a'], [2, 'b']], 
                                        schema=['c1', 'c2'])
        self.assertEqual(df.count(), 2)
        
# sys.path.append('./')

# # COMMAND ----------

# ##this import will be successful because we appended path.
# # import module1

# # COMMAND ----------

# import unittest
# # import xmlrunner
# # import coverage

# class Testmodule1(unittest.TestCase):
  
#   @classmethod
#   def setUpClass(cls):
#     cls.calculator_inst = calculator(x = 100, y = 200, spark = spark, dbutils = dbutils)

#   def setUp(self):
#     print("this is setup for every method")
#     pass

#   def test_add(self):
#     self.assertEqual(self.calculator_inst.add(10,5), 15, )

#   def test_subtract(self):
#     self.assertEqual(self.calculator_inst.subtract(10,5), 5)
#     self.assertNotEqual(self.calculator_inst.subtract(10,2), 4)

#   def test_multiply(self):
#     self.assertEqual(self.calculator_inst.multiply(10,5), 50)

#   def tearDown(self):
#     print("teardown for every method")
#     pass

#   @classmethod
#   def tearDownClass(cls):
#     print("this is teardown class")
#     pass
    

# suite = unittest.TestLoader().loadTestsFromTestCase(Testmodule1)
# runner = unittest.TextTestRunner(verbosity=2)
# runner.run(suite)
# # cov = coverage.Coverage()
# # cov.start()

# # suite =  unittest.TestLoader().loadTestsFromTestCase(Testmodule1)
# # runner = xmlrunner.XMLTestRunner(output='/dbfs/testreport.xml')
# # runner.run(suite)

# # cov.stop()
# # cov.save()
# # cov.html_report(directory='/dbfs/covhtml')
    

# COMMAND ----------


