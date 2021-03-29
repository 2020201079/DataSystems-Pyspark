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

pandas_df=df_country_count.toPandas()

with open(outputFilePath, 'a') as f:
    f.write(
        pandas_df.to_string(header = False, index = False)
    )