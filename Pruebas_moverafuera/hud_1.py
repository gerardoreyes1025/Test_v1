from pyspark.sql import SparkSession
import os
from time import time

# Configuración inicial (global)
spark = SparkSession.builder \
    .appName("Consulta RENIEC Interactiva") \
    .config("spark.driver.memory", "8G") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

# Carga inicial con cache
df = spark.read.csv(
    "RENIEC10GB_CSV.csv",
    sep=";",
    header=True,
    inferSchema=True
).cache()
df.count()  # Fuerza el caching

def buscar_por_documento(numero):
    """Busca registros por número de documento"""
    return df.filter(df["documento"] == str(numero))

def exportar_resultados(resultado, archivo="resultado.txt"):
    """Exporta todos los campos a un archivo"""
    if os.path.exists(archivo):
        os.remove(archivo)
    
    with open(archivo, "w", encoding="utf-8") as f:
        # Encabezados
        f.write("|".join(resultado.columns) + "\n")
        # Datos
        for row in resultado.collect():
            linea = "|".join([str(row[col]) for col in resultado.columns])
            f.write(linea + "\n")

def interfaz_consulta():
    """Menú interactivo para consultas"""
    while True:
        print("\n" + "="*50)
        print(" 🏛️ CONSULTA RENIEC INTERACTIVA")
        print("="*50)
        print("1. Buscar por documento")
        print("2. Salir")
        
        opcion = input("\nSeleccione opción: ")
        
        if opcion == "1":
            doc = input("Ingrese número de documento: ").strip()
            inicio = time()
            resultados = buscar_por_documento(doc)
            print(f"\n🔍 Resultados ({time()-inicio:.2f}s):")
            resultados.show(truncate=False)
            
            if input("¿Exportar a archivo? (s/n): ").lower() == "s":
                exportar_resultados(resultados)
                print("✅ Datos exportados a 'resultado.txt'")
                
        elif opcion == "2":
            print("Saliendo...")
            break

if __name__ == "__main__":
    try:
        print("⚡ Iniciando sistema (cargando datos en cache)...")
        interfaz_consulta()
    finally:
        spark.stop()
        print("Sesión de Spark cerrada")