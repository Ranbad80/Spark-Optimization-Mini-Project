import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, count, month

import os

time1 = time.time()
spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 
spark.conf.set("spark.sql.adaptive.enabled", "true")


answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

answers_month = answersDF.withColumn('month', month('creation_date')).repartition(col('month')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF_2 = questionsDF.join(answers_month, "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - time1))
print("\n")
resultDF.explain()

