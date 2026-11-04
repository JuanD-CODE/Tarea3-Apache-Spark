from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# 1. Crear sesion de Spark
spark = SparkSession.builder \
    .appName("Exploracion_Calidad_Aire") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar los datos
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("encoding", "UTF-8") \
    .csv("query.csv")

# 3. Cantidad de registros y columnas
print("\n===== DIMENSIONES DEL DATASET =====")
print("Registros:", df.count())
print("Columnas:", len(df.columns))

# 4. Revisar valores vacios
print("\n===== VALORES VACIOS =====")

columnas_importantes = [
    "id_estacion",
    "autoridad_ambiental",
    "variable",
    "unidades",
    "a_o",
    "promedio",
    "no_de_datos"
]

for nombre in columnas_importantes:
    if nombre in df.columns:
        vacios = df.filter(
            col(nombre).isNull()
        ).count()

        print(nombre, ":", vacios)

# 5. Identificar registros duplicados
print("\n===== REGISTROS DUPLICADOS =====")

duplicados = df.count() - df.dropDuplicates().count()

print("Registros duplicados:", duplicados)

# 6. Mostrar las mediciones principales
print("\n===== DATOS DE CALIDAD DEL AIRE =====")

df.select(
    "a_o",
    "variable",
    "unidades",
    "promedio"
).show(10, truncate=False)

# 7. Estadisticas del promedio
print("\n===== ESTADISTICAS DEL PROMEDIO =====")

df.select("promedio").describe().show()

# 8. Cerrar Spark
spark.stop()
