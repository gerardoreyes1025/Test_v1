from pyspark.sql import SparkSession, functions as F
import os
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from time import time

class RENIECApp:
    def __init__(self, master):
        self.master = master
        self.spark = None
        self.df = None
        
        self.setup_ui()
        self.init_spark()

    def setup_ui(self):
        """Configura la interfaz gráfica"""
        self.master.title("HUD v1 - Consulta RENIEC")
        self.master.geometry("900x700")
        
        # Frame principal
        main_frame = ttk.Frame(self.master, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel de búsqueda
        search_frame = ttk.LabelFrame(main_frame, text="Parámetros de Búsqueda", padding=10)
        search_frame.pack(fill=tk.X, pady=5)
        
        # Tipo de búsqueda
        ttk.Label(search_frame, text="Tipo de Búsqueda:").grid(row=0, column=0, sticky=tk.W)
        self.search_type = tk.StringVar()
        search_options = ttk.Combobox(search_frame, textvariable=self.search_type, 
                                    values=["Documento", "Apellidos", "Apellidos + Nombre"])
        search_options.grid(row=0, column=1, sticky=tk.EW, padx=5)
        search_options.current(0)
        
        # Campos de entrada
        self.documento_entry = self.create_search_field(search_frame, "Documento:", 1)
        self.paterno_entry = self.create_search_field(search_frame, "Paterno:", 2)
        self.materno_entry = self.create_search_field(search_frame, "Nombres:", 3)
        self.nombres_entry = self.create_search_field(search_frame, "Materno:", 4)
        
        # Botón de búsqueda
        ttk.Button(search_frame, text="Buscar", command=self.execute_search).grid(row=5, column=0, columnspan=2, pady=10)
        
        # Resultados
        results_frame = ttk.LabelFrame(main_frame, text="Resultados", padding=10)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        self.results_text = scrolledtext.ScrolledText(results_frame, height=20)
        self.results_text.pack(fill=tk.BOTH, expand=True)
        
        # Exportar
        export_frame = ttk.Frame(main_frame)
        export_frame.pack(fill=tk.X, pady=5)
        ttk.Button(export_frame, text="Exportar a TXT", command=self.export_results).pack(side=tk.LEFT)
        
        # Status
        self.status_var = tk.StringVar()
        self.status_var.set("Listo")
        ttk.Label(main_frame, textvariable=self.status_var).pack(side=tk.BOTTOM, fill=tk.X)
        
        # Actualizar visibilidad de campos
        self.search_type.trace_add("write", self.update_search_fields)

    def create_search_field(self, frame, label, row):
        """Crea campos de entrada etiquetados"""
        ttk.Label(frame, text=label).grid(row=row, column=0, sticky=tk.W)
        entry = ttk.Entry(frame)
        entry.grid(row=row, column=1, sticky=tk.EW, padx=5, pady=2)
        frame.columnconfigure(1, weight=1)
        return entry

    def update_search_fields(self, *args):
        """Muestra/oculta campos según tipo de búsqueda"""
        search_type = self.search_type.get()
        
        self.documento_entry.grid_remove()
        self.paterno_entry.grid_remove()
        self.materno_entry.grid_remove()
        self.nombres_entry.grid_remove()
        
        if search_type == "Documento":
            self.documento_entry.grid()
        elif search_type == "Apellidos":
            self.paterno_entry.grid()
            self.materno_entry.grid()
        else:  # Apellidos + Nombre
            self.paterno_entry.grid()
            self.materno_entry.grid()
            self.nombres_entry.grid()

    def init_spark(self):
        """Inicializa Spark con configuración optimizada"""
        self.status_var.set("Inicializando Spark...")
        self.master.update()
        
        try:
            self.spark = SparkSession.builder \
                .appName("HUD_v1-RENIEC") \
                .config("spark.driver.memory", "7G") \
                .config("spark.sql.shuffle.partitions", "100") \
                .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
                .getOrCreate()

            self.status_var.set("Cargando datos (37M registros)...")
            self.master.update()
            
            self.df = self.spark.read.csv(
                "RENIEC10GB_CSV.csv",
                sep=";",
                header=True,
                inferSchema=True
            ).cache()
            
            # Fuerza el caching y cuenta los registros
            total = self.df.count()
            self.status_var.set(f"✅ Sistema listo | {total:,} registros cargados")
            
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo inicializar Spark:\n{str(e)}")
            self.master.destroy()

    def execute_search(self):
        """Ejecuta la búsqueda según parámetros seleccionados"""
        search_type = self.search_type.get()
        start_time = time()
        
        try:
            if search_type == "Documento":
                doc = self.documento_entry.get().strip()
                if not doc:
                    messagebox.showwarning("Advertencia", "Ingrese un número de documento")
                    return
                
                resultados = self.df.filter(F.col("documento") == doc)
                
            elif search_type == "Apellidos":
                paterno = self.paterno_entry.get().strip().upper()
                materno = self.materno_entry.get().strip().upper()
                
                if not paterno or not materno:
                    messagebox.showwarning("Advertencia", "Ingrese ambos apellidos")
                    return
                
                resultados = self.df.filter(
                    (F.upper(F.col("paterno")) == paterno) & 
                    (F.upper(F.col("materno")) == materno)
                )
                
            else:  # Apellidos + Nombre
                paterno = self.paterno_entry.get().strip().upper()
                materno = self.materno_entry.get().strip().upper()
                nombres = self.nombres_entry.get().strip().upper()
                
                if not paterno or not materno or not nombres:
                    messagebox.showwarning("Advertencia", "Complete todos los campos")
                    return
                
                # Búsqueda flexible: encuentra si alguno de los nombres coincide
                resultados = self.df.filter(
                    (F.upper(F.col("paterno")) == paterno) & 
                    (F.upper(F.col("materno")) == materno) &
                    (F.upper(F.col("nombres")).contains(nombres))
                )
            
            # Mostrar resultados
            count = resultados.count()
            elapsed = time() - start_time
            
            self.results_text.delete(1.0, tk.END)
            self.results_text.insert(tk.END, f"⚡ Búsqueda completada en {elapsed:.2f}s\n")
            self.results_text.insert(tk.END, f"📊 Registros encontrados: {count}\n\n")
            
            if count > 0:
                # Limitar a 100 registros para visualización
                preview = resultados.limit(100).toPandas()
                self.results_text.insert(tk.END, preview.to_string())
                
                # Guardar referencia a los resultados completos
                self.last_results = resultados
            else:
                self.last_results = None
                
        except Exception as e:
            messagebox.showerror("Error", f"Error en la búsqueda:\n{str(e)}")

    def export_results(self):
        """Exporta los resultados a archivo"""
        if not hasattr(self, 'last_results') or self.last_results is None:
            messagebox.showwarning("Advertencia", "No hay resultados para exportar")
            return
            
        try:
            output_path = "resultados_reniec.txt"
            self.status_var.set("Exportando resultados...")
            self.master.update()
            
            start_time = time()
            self.last_results.write \
                .option("header", "true") \
                .option("delimiter", "|") \
                .mode("overwrite") \
                .csv("temp_results")
            
            # Unir los archivos particionados
            with open(output_path, "w", encoding="utf-8") as outfile:
                # Escribir encabezados
                with open("temp_results/part-00000-*.csv", "r", encoding="utf-8") as infile:
                    headers = infile.readline()
                    outfile.write(headers)
                
                # Escribir contenido
                for part_file in os.listdir("temp_results"):
                    if part_file.startswith("part-"):
                        with open(f"temp_results/{part_file}", "r", encoding="utf-8") as infile:
                            infile.readline()  # Saltar encabezado
                            outfile.writelines(infile.readlines())
            
            elapsed = time() - start_time
            self.status_var.set(f"✅ Exportado a {output_path} en {elapsed:.2f}s")
            messagebox.showinfo("Éxito", f"Resultados exportados a:\n{output_path}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al exportar:\n{str(e)}")
            self.status_var.set("Error en exportación")

    def on_closing(self):
        """Maneja el cierre de la aplicación"""
        if messagebox.askokcancel("Salir", "¿Está seguro de que desea salir?"):
            if self.spark:
                self.status_var.set("Cerrando sesión de Spark...")
                self.master.update()
                self.spark.stop()
            self.master.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = RENIECApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()