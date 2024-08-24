
import pyspark
from pyspark.sql import SparkSession

def get_csv_datafrmae (spark):
 
    df = spark.read.format("csv").option("header", "true").option("multiline", "true").option("ignoreLeadingWhiteSpace", "true")\
    .option("quote", '"').option("escape", "\\").option("escape", '"').load("C:/temp/actors.csv")

    df.write.mode("append").format("delta").option("overwriteSchema", "true").save("actors")

    df = spark.read.format("csv").option("header", "true").option("multiline", "true").option("ignoreLeadingWhiteSpace", "true")\
    .option("quote", '"').option("escape", "\\").option("escape", '"').load("C:/temp/Financials.csv")

    df.write.mode("append").format("delta").option("overwriteSchema", "true").save("financials")
    df = spark.read.format("csv").option("header", "true").option("multiline", "true").option("ignoreLeadingWhiteSpace", "true")\
    .option("quote", '"').option("escape", "\\").option("escape", '"').load("C:/temp/movies.csv")

    df.write.mode("append").format("delta").option("overwriteSchema", "true").save("movies")
    df =spark.read.format("csv").option("header", "true").option("multiline", "true").option("ignoreLeadingWhiteSpace", "true")\
    .option("quote", '"').option("escape", "\\").option("escape", '"').load("C:/temp/languages.csv")

    df.write.mode("append").format("delta").option("overwriteSchema", "true").save("languages")

    df = spark.read.format("csv").option("header", "true").option("multiline", "true").option("ignoreLeadingWhiteSpace", "true")\
    .option("quote", '"').option("escape", "\\").option("escape", '"').load("C:/temp/movie_actor.csv")

    df.write.mode("append").format("delta").option("overwriteSchema", "true").save("movie_actor")

