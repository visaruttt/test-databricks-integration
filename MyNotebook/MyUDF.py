# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession


def create_sample_df():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([('1', 'Visarut'), ('2', 'Tanin'), ('3', 'Nithiphan')], ['id', 'first_name'])
    df.show(truncate=False)
    print(df.columns)
    print(len(df.columns))
    return df


create_sample_df()
