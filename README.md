# Spark-Optimization-Mini-Project
Optimize Existing Code

# original Query 

![original](https://user-images.githubusercontent.com/83798130/166071710-daec3021-4a5b-4f2e-b5a1-07294c4091de.jpg)
the time is 1.02


# Repartition 
add repartition for the month column

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

+-----------+--------------------+--------------------+-----+---+
|question_id|       creation_date|               title|month|cnt|
+-----------+--------------------+--------------------+-----+---+
|     155989|2014-12-31 17:59:...|Frost bubble form...|    2|  1|
|     155989|2014-12-31 17:59:...|Frost bubble form...|   12|  1|
|     155990|2014-12-31 18:51:...|The abstract spac...|    1|  1|
|     155990|2014-12-31 18:51:...|The abstract spac...|   12|  1|
|     155992|2014-12-31 19:44:...|centrifugal force...|   12|  1|
|     155993|2014-12-31 19:56:...|How can I estimat...|    1|  1|
|     155995|2014-12-31 21:16:...|Why should a solu...|    1|  3|
|     155996|2014-12-31 22:06:...|Why do we assume ...|    1|  2|
|     155996|2014-12-31 22:06:...|Why do we assume ...|    2|  1|
|     155996|2014-12-31 22:06:...|Why do we assume ...|   11|  1|
|     155997|2014-12-31 22:26:...|Why do square sha...|    1|  3|
|     155999|2014-12-31 23:01:...|Diagonalizability...|    1|  1|
|     156008|2015-01-01 00:48:...|Capturing a light...|    1|  2|
|     156008|2015-01-01 00:48:...|Capturing a light...|   11|  1|
|     156016|2015-01-01 02:31:...|The interference ...|    1|  1|
|     156020|2015-01-01 03:19:...|What is going on ...|    1|  1|
|     156021|2015-01-01 03:21:...|How to calculate ...|    2|  1|
|     156022|2015-01-01 03:55:...|Advice on Major S...|    1|  1|
|     156025|2015-01-01 04:32:...|Deriving the Cano...|    1|  1|
|     156026|2015-01-01 04:49:...|Does Bell's inequ...|    1|  3|
+-----------+--------------------+--------------------+-----+---+
only showing top 20 rows

Processing time: **0.5436949729919434 seconds**


== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [question_id#298L, creation_date#300, title#301, month#314, cnt#330L]
   +- BroadcastHashJoin [question_id#298L], [question_id#286L], Inner, BuildRight, false
      :- Filter isnotnull(question_id#298L)
      :  +- FileScan parquet [question_id#298L,creation_date#300,title#301] Batched: true, DataFilters: [isnotnull(question_id#298L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/raniabadr/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#729]
         +- HashAggregate(keys=[question_id#286L, month#314], functions=[count(1)])
            +- HashAggregate(keys=[question_id#286L, month#314], functions=[partial_count(1)])
               +- Exchange hashpartitioning(month#314, 200), REPARTITION_BY_COL, [id=#721]
                  +- Project [question_id#286L, month(cast(creation_date#288 as date)) AS month#314]
                     +- Filter isnotnull(question_id#286L)
                        +- FileScan parquet [question_id#286L,creation_date#288] Batched: true, DataFilters: [isnotnull(question_id#286L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/raniabadr/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
                        
# broadcast

answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF_2 = questionsDF.join(broadcast(answers_month), "question_id").select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF.orderBy('question_id', 'month').show()

print("Processing time: %s seconds" % (time.time() - time1))
print("\n")
resultDF.explain()


+-----------+--------------------+--------------------+-----+---+
|question_id|       creation_date|               title|month|cnt|
+-----------+--------------------+--------------------+-----+---+
|     155989|2014-12-31 17:59:...|Frost bubble form...|    2|  1|
|     155989|2014-12-31 17:59:...|Frost bubble form...|   12|  1|
|     155990|2014-12-31 18:51:...|The abstract spac...|    1|  1|
|     155990|2014-12-31 18:51:...|The abstract spac...|   12|  1|
|     155992|2014-12-31 19:44:...|centrifugal force...|   12|  1|
|     155993|2014-12-31 19:56:...|How can I estimat...|    1|  1|
|     155995|2014-12-31 21:16:...|Why should a solu...|    1|  3|
|     155996|2014-12-31 22:06:...|Why do we assume ...|    1|  2|
|     155996|2014-12-31 22:06:...|Why do we assume ...|    2|  1|
|     155996|2014-12-31 22:06:...|Why do we assume ...|   11|  1|
|     155997|2014-12-31 22:26:...|Why do square sha...|    1|  3|
|     155999|2014-12-31 23:01:...|Diagonalizability...|    1|  1|
|     156008|2015-01-01 00:48:...|Capturing a light...|    1|  2|
|     156008|2015-01-01 00:48:...|Capturing a light...|   11|  1|
|     156016|2015-01-01 02:31:...|The interference ...|    1|  1|
|     156020|2015-01-01 03:19:...|What is going on ...|    1|  1|
|     156021|2015-01-01 03:21:...|How to calculate ...|    2|  1|
|     156022|2015-01-01 03:55:...|Advice on Major S...|    1|  1|
|     156025|2015-01-01 04:32:...|Deriving the Cano...|    1|  1|
|     156026|2015-01-01 04:49:...|Does Bell's inequ...|    1|  3|
+-----------+--------------------+--------------------+-----+---+
only showing top 20 rows

Processing time: **0.5546250343322754 seconds**


== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [question_id#298L, creation_date#300, title#301, month#314, cnt#330L]
   +- BroadcastHashJoin [question_id#298L], [question_id#286L], Inner, BuildRight, false
      :- Filter isnotnull(question_id#298L)
      :  +- FileScan parquet [question_id#298L,creation_date#300,title#301] Batched: true, DataFilters: [isnotnull(question_id#298L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/raniabadr/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#729]
         +- HashAggregate(keys=[question_id#286L, month#314], functions=[count(1)])
            +- HashAggregate(keys=[question_id#286L, month#314], functions=[partial_count(1)])
               +- Exchange hashpartitioning(month#314, 200), REPARTITION_BY_COL, [id=#721]
                  +- Project [question_id#286L, month(cast(creation_date#288 as date)) AS month#314]
                     +- Filter isnotnull(question_id#286L)
                        +- FileScan parquet [question_id#286L,creation_date#288] Batched: true, DataFilters: [isnotnull(question_id#286L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/raniabadr/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
                        


# cache()

answers_month.cache()

+-----------+--------------------+--------------------+-----+---+
|question_id|       creation_date|               title|month|cnt|
+-----------+--------------------+--------------------+-----+---+
|     155989|2014-12-31 17:59:...|Frost bubble form...|    2|  1|
|     155989|2014-12-31 17:59:...|Frost bubble form...|   12|  1|
|     155990|2014-12-31 18:51:...|The abstract spac...|    1|  1|
|     155990|2014-12-31 18:51:...|The abstract spac...|   12|  1|
|     155992|2014-12-31 19:44:...|centrifugal force...|   12|  1|
|     155993|2014-12-31 19:56:...|How can I estimat...|    1|  1|
|     155995|2014-12-31 21:16:...|Why should a solu...|    1|  3|
|     155996|2014-12-31 22:06:...|Why do we assume ...|    1|  2|
|     155996|2014-12-31 22:06:...|Why do we assume ...|    2|  1|
|     155996|2014-12-31 22:06:...|Why do we assume ...|   11|  1|
|     155997|2014-12-31 22:26:...|Why do square sha...|    1|  3|
|     155999|2014-12-31 23:01:...|Diagonalizability...|    1|  1|
|     156008|2015-01-01 00:48:...|Capturing a light...|    1|  2|
|     156008|2015-01-01 00:48:...|Capturing a light...|   11|  1|
|     156016|2015-01-01 02:31:...|The interference ...|    1|  1|
|     156020|2015-01-01 03:19:...|What is going on ...|    1|  1|
|     156021|2015-01-01 03:21:...|How to calculate ...|    2|  1|
|     156022|2015-01-01 03:55:...|Advice on Major S...|    1|  1|
|     156025|2015-01-01 04:32:...|Deriving the Cano...|    1|  1|
|     156026|2015-01-01 04:49:...|Does Bell's inequ...|    1|  3|
+-----------+--------------------+--------------------+-----+---+
only showing top 20 rows

Processing time: **1.042649745941162 seconds**


== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [question_id#298L, creation_date#300, title#301, month#314, cnt#330L]
   +- BroadcastHashJoin [question_id#298L], [question_id#286L], Inner, BuildRight, false
      :- Filter isnotnull(question_id#298L)
      :  +- FileScan parquet [question_id#298L,creation_date#300,title#301] Batched: true, DataFilters: [isnotnull(question_id#298L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/raniabadr/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#729]
         +- HashAggregate(keys=[question_id#286L, month#314], functions=[count(1)])
            +- HashAggregate(keys=[question_id#286L, month#314], functions=[partial_count(1)])
               +- Exchange hashpartitioning(month#314, 200), REPARTITION_BY_COL, [id=#721]
                  +- Project [question_id#286L, month(cast(creation_date#288 as date)) AS month#314]
                     +- Filter isnotnull(question_id#286L)
                        +- FileScan parquet [question_id#286L,creation_date#288] Batched: true, DataFilters: [isnotnull(question_id#286L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/raniabadr/data/answers], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
                        
                        
# conclusion
the best performance is coming fron repartition month 
