# ===================================
# DATAFRAME MANIPULATION - PYSPARK
# SELECT [x]
# FROM [x]
# WHERE [X] filter + where
# CASE WHEN [X]
# ORDER BY [X]
# COALESCE() / LOWER() / COUNT() / SPLIT_TEXT []
# APPLY FUNCTIONs []
# CTES []
# JOINS []
# GROUPBY/HAVING []
# WINDOW FUNCTIONS [] ROW_NUMBER / RANK / DENSE RANK
# ===================================
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col

spark = SparkSession.builder.master("spark://spark-master:7077").appName('dataFrameManipulation').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the CSV file
df = spark.read.csv("file:/workspace/data/googleplaystore_user_reviews.csv", header=True, inferSchema=True)

# SELECT / SELECT DISTINCT
distincts_apps = df.select('App').distinct()
distincts_sentiment_list = list(df.select('Sentiment').distinct().toPandas()['Sentiment'])

# FILTER
df = df.filter(col("Sentiment_Polarity").contains('.')).select('App', 'Translated_Review', 'Sentiment', 'Sentiment_Polarity').filter(col('Sentiment_Polarity').rlike(r"^-?\d+(\.\d+)?$"))
df = df.withColumn('Sentiment_Polarity',col('Sentiment_Polarity').cast('double')).filter(col('Sentiment_Polarity').isNotNull())

# GROUP BY
df_polarity_mm = df.groupBy("Sentiment").agg(
    F.max('Sentiment_Polarity').alias('Sentiment_Polarity_Max'),
    F.min('Sentiment_Polarity').alias('Sentiment_Polarity_Min'),
    (F.max('Sentiment_Polarity') - F.min('Sentiment_Polarity')).alias('Sentiment_Polarity_Amplitude'),
    F.avg('Sentiment_Polarity').alias('Sentiment_Polarity_Avg')
).filter(col('Sentiment_Polarity_Avg') != 0)     # df.groupBy(col('Sentiment')).max().show() it will show just one
df_polarity_mm.show()

# CASE WHEN + GROUP BY TO DEDUPE
df_factor_sentiment = df.select('Sentiment').withColumn("Sentiment_factor", F.when(col('Sentiment')=="Positive","1")
                                                       .when(col('Sentiment')== "Neutral","0")
                                                       .when(col('Sentiment')=="Negative","-1")
                                                       .otherwise('-999')
                                                       ).withColumn('Sentiment_factor',col('Sentiment_factor').cast('integer')).groupBy('Sentiment').agg(F.max('Sentiment_factor').alias('Sentiment_factor'))
df_factor_sentiment.show()

# CASE WHEN + DISTINCT TO DEDUPE +  ORDER BY
df_factor_sentiment_distict = df.select('Sentiment').withColumn("Sentiment_factor", F.when(col('Sentiment')=='Positive','1')
                                                                                           .when(col('Sentiment')=='Neutral','0.5')
                                                                                           .when(col('Sentiment')=='Negative','-1')
                                                                                           .otherwise('-999')).select("Sentiment","Sentiment_factor").distinct().withColumn('Sentiment_factor',col('Sentiment_factor')).orderBy(["Sentiment_factor"],ascending=[True])
df_factor_sentiment_distict.show()

# JOINs
df1 = df_factor_sentiment.alias('df1')
df2 = df_factor_sentiment_distict.alias('df2')
# inner join
df_inner_join = df1.join(df2, df1.Sentiment == df2.Sentiment,'inner').select(
    col('df1.Sentiment').alias('Sentiment_1'),
    col('df1.Sentiment_factor').alias('Sentiment_factor_1'),
    col('df2.Sentiment').alias('Sentiment_2'),
    col('df2.Sentiment_factor').cast('double').alias('Sentiment_factor_2')
)
df_inner_join.show()
# left join as alternative to inner join
df_11 = df1.withColumn('Expression', F.lit('Yes')).alias('df_11')
df_22 = df2.withColumn('Expression', F.lit('No')).alias('df_22')
df_inner_left_join = df_11.join(df_22,(col("df_11.Sentiment") == col("df_22.Sentiment")) & (col('df_11.Sentiment_factor')==col('df_22.Sentiment_factor').cast('double')),'left').select(
    col('df_11.Sentiment'),
    col('df_11.Sentiment_factor').cast('integer').alias('Sentiment_factor'),
    col('df_11.Expression').alias('Expression_1'),
    col('df_22.Expression').alias('Expression_2')
).filter(col('Expression_2').isNotNull()).orderBy(['Sentiment_factor'], ascending=[False])
df_inner_left_join.show()

# pure left join -> LEFT JOIN COALESCE + CASE WHEN


# cross join

#df.show()










# python first_pyspark.py




