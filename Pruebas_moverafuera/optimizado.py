from pyspark.sql import SparkSession
import os
from time import time

def init_spark():
    """Configuración optimizada de Spark para 8GB RAM"""
    return SparkSession.builder \
        .appName("Consulta RENIEC Optimizada") \
        .config("spark.driver.memory", "8G") \
        .config("spark.executor.memory", "8G") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .getOrCreate()

def load_data(spark):
    """Carga optimizada de datos con inferencia de esquema"""
    return spark.read.csv(
        "RENIEC10GB_CSV.csv",
        sep=";",
        header=True,
        inferSchema=True,
        nullValue="NULL"  # Manejo explícito de valores nulos
    )

def export_full_results(df, output_path="output_full.txt"):
    """Exporta todas las columnas a un archivo estructurado"""
    if os.path.exists(output_path):
        os.remove(output_path)
        
    # Obtener todas las columnas
    all_columns = df.columns
    
    # Escribir encabezados
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("|".join(all_columns) + "\n")  # Usamos pipe como separador
    
    # Escribir datos
    with open(output_path, "a", encoding="utf-8") as f:
        for row in df.collect():
            values = [str(row[col]) if row[col] is not None else "" for col in all_columns]
            f.write("|".join(values) + "\n")

def main():
    start_time = time()
    spark = init_spark()
    
    try:
        # Carga optimizada con caché (solo si harás múltiples consultas)
        df = load_data(spark)
        
        # # 2. 👇 ACTIVAR CACHE SOLO PARA CONSULTAS MÚLTIPLES (descomenta estas 2 líneas)
        # print("🔄 Activando cache...")
        # df.cache()  # Almacena el DataFrame en memoria
        # df.count()  # Fuerza la carga inmediata (opcional pero recomendado)

        # EJEMPLOS DE CONSULTAS (descomenta la que necesites)
        # Consulta por documento
        resultado = df.filter(df["documento"] == "22502321")
        
        # Consulta por nombre completo
        # resultado = df.filter(
        #     (df["paterno"] == "TEJADA") & 
        #     (df["materno"] == "LLAMOGA") &
        #     (df["nombres"] == "GERARDO SEBASTIAN")
        # )
        
        # Consulta por apellidos
        # resultado = df.filter(
        #     (df["paterno"] == "TEJADA") & 
        #     (df["materno"] == "LLAMOGA")
        # )
        
        # Mostrar preview en consola
        print("\n🔍 Resultados encontrados:")
        resultado.show(5, truncate=False, vertical=True)
        
        # Exportar todos los resultados con todas las columnas
        export_full_results(resultado)
        
        print(f"\n✅ Exportados {resultado.count()} registros a output_full.txt")
        print(f"⏱️ Tiempo total: {time()-start_time:.2f} segundos")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()