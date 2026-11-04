from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

# 1. Crear sesion de Spark
spark = SparkSession.builder \
    .appName("Limpieza_Calidad_Aire") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar archivo original
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("encoding", "UTF-8") \
    .csv("query.csv")

# 3. Mostrar cantidad inicial de registros
print("\n===== DATOS ORIGINALES =====")
print("Registros iniciales:", df.count())

# 4. Eliminar registros duplicados
df_limpio = df.dropDuplicates()

# 5. Eliminar registros sin datos esenciales
columnas_clave = [
    "estaci_n",
    "variable",
    "unidades",
    "a_o",
    "promedio"
]

df_limpio = df_limpio.dropna(
    subset=columnas_clave
)

# 6. Normalizar los nombres de las variables
df_limpio = df_limpio.withColumn(
    "variable",
    upper(trim(col("variable")))
)

# 7. Mostrar resultados de la limpieza
print("\n===== RESULTADOS DE LIMPIEZA =====")
print("Registros originales:", df.count())
print("Registros limpios:", df_limpio.count())
print(
    "Registros descartados:",
    df.count() - df_limpio.count()
)

# 8. Mostrar datos limpios
print("\n===== DATOS LIMPIOS =====")

df_limpio.select(
    "estaci_n",
    "variable",
    "unidades",
    "a_o",
    "promedio"
).show(10, truncate=False)

# 9. Guardar conjunto de datos limpio
df_limpio.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("calidad_aire_limpio")

print("\nArchivo limpio guardado correctamente.")

spark.stop()
