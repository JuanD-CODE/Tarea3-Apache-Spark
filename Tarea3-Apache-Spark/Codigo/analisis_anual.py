from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count, round
)

# 1. Crear sesion de Spark
spark = SparkSession.builder \
    .appName("Analisis_Anual_Calidad_Aire") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar los datos originales
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("encoding", "UTF-8") \
    .csv("query.csv")

# 3. Eliminar duplicados y registros incompletos
df_limpio = df.dropDuplicates().dropna(
    subset=["variable", "unidades", "a_o", "promedio"]
)

# 4. Agrupar por año, variable y unidad
analisis = df_limpio.groupBy(
    "a_o", "variable", "unidades"
).agg(
    count("*").alias("cantidad_registros"),
    round(avg("promedio"), 2).alias("promedio_anual"),
    round(min("promedio"), 2).alias("valor_minimo"),
    round(max("promedio"), 2).alias("valor_maximo")
)

# 5. Ordenar resultados
analisis = analisis.orderBy(
    "a_o", "variable"
)

# 6. Mostrar resultados
print("\n===== ANALISIS ANUAL DE CALIDAD DEL AIRE =====")

analisis.show(50, truncate=False)

# 7. Guardar resultados
analisis.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("resultados_analisis_anual")

print("\nResultados guardados correctamente.")

# 8. Cerrar Spark
spark.stop()
