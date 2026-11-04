from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# 1. Crear sesion de Spark
spark = SparkSession.builder \
    .appName("Graficas_Calidad_Aire") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leer los resultados del analisis anual
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("resultados_analisis_anual")

# 3. Seleccionar los contaminantes
contaminantes = ["PM10", "PM2.5"]

for contaminante in contaminantes:

    datos = df.filter(
        col("variable") == contaminante
    ).orderBy("a_o")

    # Convertir los resultados para graficar
    filas = datos.select(
        "a_o", "promedio_anual"
    ).collect()

    anios = [fila["a_o"] for fila in filas]
    promedios = [fila["promedio_anual"] for fila in filas]

    # 4. Crear grafica
    plt.figure(figsize=(10, 5))

    plt.plot(
        anios,
        promedios,
        marker="o",
        linewidth=2
    )

    plt.title(
        f"Evolucion anual de {contaminante} en Colombia"
    )

    plt.xlabel("Año")
    plt.ylabel("Promedio anual (ugm3)")
    plt.xticks(anios, rotation=45)
    plt.grid(True)
    plt.tight_layout()

    # 5. Guardar imagen
    nombre = f"grafico_{contaminante.replace('.', '_')}.png"

    plt.savefig(nombre, dpi=150)
    plt.close()

    print(f"Grafica generada: {nombre}")

# 6. Cerrar Spark
spark.stop()
