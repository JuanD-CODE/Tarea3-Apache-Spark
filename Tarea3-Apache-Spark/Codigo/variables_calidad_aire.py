from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
    .appName("Variables_Calidad_Aire") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("encoding", "UTF-8") \
    .csv("query.csv")

print("\n===== VARIABLES REGISTRADAS =====")

df.groupBy("variable", "unidades") \
    .agg(count("*").alias("cantidad_registros")) \
    .orderBy("variable") \
    .show(50, truncate=False)

print("\n===== REGISTROS POR AÑO =====")

df.groupBy("a_o") \
    .agg(count("*").alias("cantidad_registros")) \
    .orderBy("a_o") \
    .show(50)

spark.stop()
