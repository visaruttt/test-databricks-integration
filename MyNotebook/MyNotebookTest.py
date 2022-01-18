# Databricks notebook source
# Cmd1 
dbutils.notebook.run("MyNotebook", 10)

# COMMAND ----------

# Cmd2
import unittest

class MyNotebookTests(unittest.TestCase):
  
  def test_hoge(self):
    self.assertEqual(hoge(3), 'hogehogehoge')
    self.assertNotEqual(hoge(2), 'hoge')

  def test_fuga(self):
    self.assertEqual(fuga(3), 'fugafugafuga')
    self.assertNotEqual(fuga(2), 'fuga')

suite = unittest.TestLoader().loadTestsFromTestCase(MyNotebookTests)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# COMMAND ----------


