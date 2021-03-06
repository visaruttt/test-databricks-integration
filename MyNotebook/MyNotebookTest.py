# Databricks notebook source
# MAGIC %run ./MyNotebook
import unittest


class MyNotebookTests(unittest.TestCase):
    def test_simple_repeat_word(self):
        self.assertEqual(simple_repeat_word(3), 'wording wording wording ')
        self.assertNotEqual(simple_repeat_word(2), 'wording')


suite = unittest.TestLoader().loadTestsFromTestCase(MyNotebookTests)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
