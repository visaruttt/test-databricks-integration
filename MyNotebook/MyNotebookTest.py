# Databricks notebook source
# MAGIC %run ./MyNotebook

# COMMAND ----------

import unittest


class MyNotebookTests(unittest.TestCase):
    def test_simple_repeat_word(self):
        self.assertEqual(simple_repeat_word(3), 'wording wording wording ')
        self.assertNotEqual(simple_repeat_word(2), 'wording')


suite = unittest.TestLoader().loadTestsFromTestCase(MyNotebookTests)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# COMMAND ----------

# import sys
# sys.path.append('./MyNotebook')

# import MyNotebook

# import unittest
# # import xmlrunner
# # import coverage

# class Testmodule1(unittest.TestCase):
    
#     @classmethod
#     def setUpClass(cls):
#         cls.calculator_inst = customsparktest.calculator(x = 100, y = 200, spark = spark, dbutils = dbutils)

#     def setUp(self):
#         print("this is setup for every method")
#         pass

#     def test_simple_repeat_word(self):
#         self.assertEqual(simple_repeat_word(3), 'wording wording wording ')
#         self.assertNotEqual(simple_repeat_word(2), 'wording')


# #   def setUpClass(cls):
# #     cls.calculator_inst = customsparktest.calculator(x = 100, y = 200, spark = spark, dbutils = dbutils)

# #   def setUp(self):
# #     print("this is setup for every method")
# #     pass

# #   def test_add(self):
# #     self.assertEqual(self.calculator_inst.add(10,5), 15, )

# #   def test_subtract(self):
# #     self.assertEqual(self.calculator_inst.subtract(10,5), 5)
# #     self.assertNotEqual(self.calculator_inst.subtract(10,2), 4)

# #   def test_multiply(self):
# #     self.assertEqual(self.calculator_inst.multiply(10,5), 50)

# #   def tearDown(self):
# #     print("teardown for every method")
# #     pass

# #   @classmethod
# #   def tearDownClass(cls):
# #     print("this is teardown class")
# #     pass
    

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


