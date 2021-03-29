import findspark
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,asc,desc
import pandas as pd



findspark.find()
findspark.init()

argumentList = sys.argv
print(argumentList)
if(len(argumentList)!=3):
    print("2 arguments shold be provided")
    exit()

numberOfCPU = int(argumentList[1])
outputFilePath = argumentList[2]

spark = SparkSession.builder.appName("assign5").config("spark.executor.instances", numberOfCPU).config("spark.executor.cores", numberOfCPU).getOrCreate()

df = spark.read.csv("airports.csv",header=True)

df_country_count = df.groupBy("COUNTRY").count()

df_country_count_sorted = df_country_count.sort(col("count").desc()).limit(3)

pandas_df=df_country_count_sorted.toPandas()

with open(outputFilePath, 'a') as f:
    f.write(
        pandas_df.to_string(header = False, index = False)
    )