from pyspark.sql import SparkSession

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Analisis_Calidad_Aire_Colombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el archivo CSV
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("encoding", "UTF-8") \
    .csv("query.csv")

# 3. Mostrar información general
print("\n========== INFORMACION DEL DATASET ==========")

print("Total de registros:", df.count())
print("Total de columnas:", len(df.columns))

# 4. Mostrar los nombres de las columnas
print("\n========== COLUMNAS ==========")
print(df.columns)

# 5. Mostrar los tipos de datos
print("\n========== ESQUEMA DEL DATASET ==========")
df.printSchema()

# 6. Mostrar los primeros cinco registros
print("\n========== PRIMEROS CINCO REGISTROS ==========")
df.show(5, truncate=False)

# 7. Finalizar Spark
spark.stop()
