# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 16:05:44 2021

@author: Talha
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    
df = spark.read.csv("C:/Users/Dell/Desktop/Talha/hepsiburada/exhibitA-input.csv",header=True,sep = '\t')
df.createOrReplaceTempView("song")
sqlDF1 = spark.sql("SELECT COUNT(DISTINCT PLAY_ID) FROM song WHERE SONG_ID = 346 GROUP BY SONG_ID")

sqlDF2 = spark.sql("SELECT COUNT(DISTINCT PLAY_ID) FROM song GROUP BY SONG_ID ORDER BY 1 DESC")

sqlDF1.show()
sqlDF2.show()