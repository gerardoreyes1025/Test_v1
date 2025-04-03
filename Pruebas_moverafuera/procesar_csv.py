# Importar librerías
from pyspark.sql import SparkSession
import os

# Iniciar Spark (se ejecutará localmente)
spark = SparkSession.builder \
    .appName("Procesar CSV Grande") \
    .config("spark.driver.memory", "8G") \
    .config("spark.hadoop.validateOutputSpecs", "false") \
    .getOrCreate()

# Cargar el CSV
df = spark.read.csv(
    "RENIEC10GB_CSV.csv",  # Usa rutas con "/" o "\\"
    sep=";",               # Separador explícito
    header=True,           # Usar primera fila como encabezado
    inferSchema=True       # Inferir tipos de datos
)

# Filtrar datos
resultado = df.filter(
    # (df["paterno"] == "TEJADA") & 
    # (df["materno"] == "LLAMOGA")&
    # (df["nombres"] == "GERARDO SEBASTIAN")

    # (df["paterno"] == "TEJADA") & 
    # (df["materno"] == "LLAMOGA")

    (df["documento"] == "22502323")
)

# Mostrar en consola (primeras 5 filas)
resultado.select("nombres", "documento", "direccion", "telefono").show(5)

# Exportar a archivo TXT (se sobrescribirá cada vez)
output_path = "output.txt"

# Eliminar archivo existente si hay
if os.path.exists(output_path):
    os.remove(output_path)

# Escribir encabezados
with open(output_path, "w", encoding="utf-8") as f:
    f.write("nombres\tdocumento\tdireccion\ttelefono\n")

# Escribir resultados
with open(output_path, "a", encoding="utf-8") as f:
    for row in resultado.select("nombres", "documento", "direccion", "telefono").collect():
        f.write(f"{row['nombres']}\t{row['documento']}\t{row['direccion']}\t{row['telefono']}\n")

print(f"\nResultados exportados a {output_path}")

spark.stop()