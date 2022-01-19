# Databricks notebook source
# MAGIC %run ./MyUDF
import unittest
import coverage
import xmlrunner


class MyNotebookTests(unittest.TestCase):
    def test_simple_repeat_word(self):
        self.assertEqual(len(create_sample_df().columns), 2)
        self.assertNotEqual(len(create_sample_df().columns), 10)


cov = coverage.Coverage("/tmp/.coverage")
cov.start()

suite = unittest.TestLoader().loadTestsFromTestCase(MyNotebookTests)
runner = xmlrunner.XMLTestRunner(output='/tmp/testreport.xml')
runner.run(suite)

cov.stop()
print(cov)
cov.save()
cov.html_report(directory='/tmp/covhtml')
