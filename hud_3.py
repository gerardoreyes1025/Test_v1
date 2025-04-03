from pyspark.sql import SparkSession, functions as F
import os
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from time import time
from threading import Thread
from datetime import datetime
import pandas as pd



class LoadingWindow:
    """Ventana de carga durante la inicialización"""
    def __init__(self, parent):
        self.top = tk.Toplevel(parent)
        self.top.title("Cargando...")
        self.top.geometry("300x100")
        self.top.resizable(False, False)
        
        # Centrar la ventana
        parent.update_idletasks()
        p_width = parent.winfo_width()
        p_height = parent.winfo_height()
        p_x = parent.winfo_x()
        p_y = parent.winfo_y()
        self.top.geometry(f"+{p_x + p_width//2 - 150}+{p_y + p_height//2 - 50}")
        
        # Contenido
        ttk.Label(self.top, text="Inicializando sistema...", font=('Arial', 10)).pack(pady=10)
        self.progress = ttk.Progressbar(self.top, mode='indeterminate')
        self.progress.pack(fill=tk.X, padx=20)
        self.progress.start()
        
        # Bloquear interacción con la ventana principal
        self.top.grab_set()
        self.top.transient(parent)
        
    def close(self):
        self.progress.stop()
        self.top.destroy()

class RENIECApp:
    def __init__(self, master):
        self.master = master
        self.spark = None
        self.df = None
        self.current_results = None
        self.filter_options = {}
        self.loading_window = LoadingWindow(master)
        
        # Iniciar carga en segundo plano
        Thread(target=self.initialize_system, daemon=True).start()
        
    def initialize_system(self):
        """Inicializa el sistema en segundo plano"""
        try:
            self.setup_ui()
            self.init_spark()
        except Exception as e:
            messagebox.showerror("Error", f"Error de inicialización:\n{str(e)}")
            self.master.after(0, self.master.destroy)
        finally:
            self.master.after(0, self.loading_window.close)

    def setup_ui(self):
        """Configura la interfaz gráfica con nuevos filtros"""
        self.master.title("RENIEC LEAKED 31.9M 2023")
        self.master.geometry("1000x800")
        
        # Frame principal
        main_frame = ttk.Frame(self.master, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel de búsqueda
        search_frame = ttk.LabelFrame(main_frame, text="Parámetros de Búsqueda", padding=10)
        search_frame.pack(fill=tk.X, pady=5)
        
        # Tipo de búsqueda
        ttk.Label(search_frame, text="Tipo de Búsqueda:").grid(row=0, column=0, sticky=tk.W)
        self.search_type = tk.StringVar(value="Documento")
        search_options = ttk.Combobox(
            search_frame, 
            textvariable=self.search_type, 
            values=["Documento", "Apellidos", "Apellidos + Nombre"],
            state="readonly"
        )
        search_options.grid(row=0, column=1, sticky=tk.EW, padx=5)
        
        # Campos de entrada
        self.documento_entry = self.create_search_field(search_frame, "Documento:", 1)
        self.paterno_entry = self.create_search_field(search_frame, "Apellido Paterno:", 2)
        self.materno_entry = self.create_search_field(search_frame, "Apellido Materno:", 3)
        self.nombres_entry = self.create_search_field(search_frame, "Nombres:", 4)
        
        # Mostrar solo documento inicialmente
        self.paterno_entry.grid_remove()
        self.materno_entry.grid_remove()
        self.nombres_entry.grid_remove()
        
        # Botón de búsqueda
        ttk.Button(search_frame, text="Buscar", command=self.execute_search).grid(
            row=5, column=0, columnspan=2, pady=10
        )
        
        # Panel de filtros
        filter_frame = ttk.LabelFrame(main_frame, text="Filtros Avanzados", padding=10)
        filter_frame.pack(fill=tk.X, pady=5)
        
        # Filtro por ubicación
        ttk.Label(filter_frame, text="Ubicación:").grid(row=0, column=0, sticky=tk.W)
        self.ubicacion_filter = ttk.Combobox(filter_frame, state="normal")
        self.ubicacion_filter.grid(row=0, column=1, sticky=tk.EW, padx=5)
        
        # Filtro por sexo
        ttk.Label(filter_frame, text="Sexo:").grid(row=0, column=2, sticky=tk.W, padx=(10,0))
        # self.sexo_filter = ttk.Combobox(filter_frame, state="normal", width=10)
        self.sexo_filter = ttk.Combobox(filter_frame, 
                                  values=[("1", "Masculino"), ("2", "Femenino")],
                                  state="readonly")
        self.sexo_filter.grid(row=0, column=3, sticky=tk.EW, padx=5)
        
        # Filtro por rango de edad
        ttk.Label(filter_frame, text="Edad entre:").grid(row=1, column=0, sticky=tk.W, pady=(5,0))
        self.edad_min_entry = ttk.Entry(filter_frame, width=5)
        self.edad_min_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=(5,0))
        ttk.Label(filter_frame, text="y").grid(row=1, column=2, sticky=tk.W, pady=(5,0))
        self.edad_max_entry = ttk.Entry(filter_frame, width=5)
        self.edad_max_entry.grid(row=1, column=3, sticky=tk.W, padx=5, pady=(5,0))
        
        # Botón de aplicar filtros
        ttk.Button(filter_frame, text="Aplicar Filtros", command=self.apply_filters).grid(
            row=1, column=4, padx=10, pady=(5,0))
        
        # Botón de limpiar filtros
        ttk.Button(filter_frame, text="Limpiar Filtros", command=self.clear_filters).grid(
            row=1, column=5, pady=(5,0))
        
        # Resultados
        results_frame = ttk.LabelFrame(main_frame, text="Resultados", padding=10)
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        self.results_text = scrolledtext.ScrolledText(
            results_frame, 
            height=20,
            wrap=tk.NONE,
            font=('Consolas', 10)
        )
        self.results_text.pack(fill=tk.BOTH, expand=True)
        
        # Barra de desplazamiento horizontal
        h_scroll = ttk.Scrollbar(results_frame, orient='horizontal', command=self.results_text.xview)
        h_scroll.pack(fill=tk.X)
        self.results_text.config(xscrollcommand=h_scroll.set)
        
        # Exportar
        export_frame = ttk.Frame(main_frame)
        export_frame.pack(fill=tk.X, pady=5)
        ttk.Button(export_frame, text="Exportar a TXT", command=self.export_results).pack(side=tk.LEFT)
        
        # Status
        self.status_var = tk.StringVar(value="Inicializando...")
        status_label = ttk.Label(main_frame, textvariable=self.status_var)
        status_label.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Actualizar visibilidad de campos cuando cambia el tipo de búsqueda
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
        
        # Ocultar todos primero
        self.documento_entry.grid_remove()
        self.paterno_entry.grid_remove()
        self.materno_entry.grid_remove()
        self.nombres_entry.grid_remove()
        
        # Mostrar los relevantes
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

            self.status_var.set("Cargando datos (31.9M registros)...")
            self.master.update()
            
            self.df = self.spark.read.csv(
                #Aca traemos el csv
                "../RENIEC10GB_CSV.csv",
                sep=";",
                header=True,
                inferSchema=True,
                encoding="UTF-8",
                nullValue="NULL"
            ).cache()
            
            # Fuerza el caching y cuenta los registros
            total = self.df.count()
            self.status_var.set(f"✅ Sistema listo | {total:,} registros cargados")
            
            # Cargar opciones de filtro
            self.load_filter_options()
            
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo inicializar Spark:\n{str(e)}")
            self.master.destroy()

    # def load_filter_options(self):
    #     """Carga las opciones de filtro disponibles"""
    #     try:
    #         # Cargar ubicaciones únicas (muestreamos para no sobrecargar)
    #         ubicaciones = self.df.select("ubicacion").distinct().limit(1000).collect()
    #         self.ubicacion_filter['values'] = [row.ubicacion for row in ubicaciones if row.ubicacion]
            
    #         # Cargar sexos únicos
    #         sexos = self.df.select("sexo").distinct().collect()
    #         self.sexo_filter['values'] = [row.sexo for row in sexos if row.sexo]
            
    #     except Exception as e:
    #         print(f"Advertencia: No se pudieron cargar opciones de filtro: {str(e)}")

    def load_filter_options(self):
        """Carga las opciones de filtro disponibles basadas en resultados actuales"""
        try:
        #     # Usar current_results si existe, sino el dataframe completo
        #     base_df = self.current_results if self.current_results is not None else self.df

        #     # Cargar ubicaciones únicas ordenadas alfabéticamente
        #     ubicaciones = base_df.select("ubicacion").distinct().orderBy("ubicacion").limit(1000).collect()
        #     self.ubicacion_filter['values'] = [row.ubicacion for row in ubicaciones if row.ubicacion]
            
        #     # Cargar sexos únicos (solo 1 y 2)
        #     sexos = [("1", "Masculino"), ("2", "Femenino")]
        #     # self.sexo_filter['values'] = [s[0] for s in sexos]  # O usa s[1] si quieres mostrar el texto
        #     self.sexo_filter = ttk.Combobox(filter_frame, 
        #                             values=[("1", "Masculino"), ("2", "Femenino")],
        #                             state="readonly")
            
        # except Exception as e:
        #     print(f"Advertencia: No se pudieron cargar opciones de filtro: {str(e)}")
                    # Usar current_results si existe, sino no cargar nada
            # # if self.current_results is None:
            # #         return
                
            # # # Cargar ubicaciones únicas ordenadas alfabéticamente solo de los resultados actuales
            # # ubicaciones = self.current_results.select("ubicacion").distinct().orderBy("ubicacion").collect()
            if hasattr(self, 'last_search_results') and self.last_search_results is not None:
                # Cargar ubicaciones únicas ordenadas alfabéticamente
                ubicaciones = self.last_search_results.select("ubicacion").distinct().orderBy("ubicacion").collect()
                self.ubicacion_filter['values'] = [row.ubicacion for row in ubicaciones if row.ubicacion]
            
            # Actualizar el combobox de sexo con valores fijos
            self.sexo_filter['values'] = ["1", "2"]
            
        except Exception as e:
            print(f"Advertencia: No se pudieron cargar opciones de filtro: {str(e)}")

    def apply_filters(self):
        """Aplica los filtros seleccionados a los resultados actuales"""
        # if self.current_results is None:
        #     messagebox.showwarning("Advertencia", "Primero realice una búsqueda básica")
        #     return
        if not hasattr(self, 'last_search_results') or self.last_search_results is None:
            messagebox.showwarning("Advertencia", "Primero realice una búsqueda básica")
            return

        try:
            start_time = time()
            filtered_df = self.current_results
            
            # Aplicar filtro de ubicación
            ubicacion = self.ubicacion_filter.get()
            if ubicacion:
                # filtered_df = filtered_df.filter(F.col("ubicacion") == ubicacion.lower())
                filtered_df = filtered_df.filter(F.lower(F.col("ubicacion")).contains(ubicacion.lower()))
            
            # Aplicar filtro de sexo
            sexo = self.sexo_filter.get()
            if sexo:
                filtered_df = filtered_df.filter(F.col("sexo") == sexo)
            
            # Aplicar filtro de edad
            edad_min = self.edad_min_entry.get()
            edad_max = self.edad_max_entry.get()
            
            if edad_min:
                try:
                    edad_min = int(edad_min)
                    filtered_df = filtered_df.filter(F.col("edad") >= edad_min)
                except ValueError:
                    messagebox.showwarning("Advertencia", "Edad mínima debe ser un número")
                    return
                    
            if edad_max:
                try:
                    edad_max = int(edad_max)
                    filtered_df = filtered_df.filter(F.col("edad") <= edad_max)
                except ValueError:
                    messagebox.showwarning("Advertencia", "Edad máxima debe ser un número")
                    return
            
            # Mostrar resultados filtrados
            count = filtered_df.count()
            elapsed = time() - start_time
            
            self.results_text.config(state=tk.NORMAL)
            self.results_text.delete(1.0, tk.END)
            self.results_text.insert(tk.END, f"⚡ Filtros aplicados en {elapsed:.2f}s\n")
            self.results_text.insert(tk.END, f"📊 Registros encontrados: {count}\n\n")
            
            if count > 0:
                preview = filtered_df.limit(100).toPandas()
                self.results_text.insert(tk.END, preview.to_string(index=False))
                self.current_results = filtered_df
                # self.load_filter_options()  # Actualizar opciones de filtro basadas en nuevos resultados
            else:
                messagebox.showinfo("Información", "No hay resultados con los filtros aplicados")
                
            self.results_text.config(state=tk.DISABLED)
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al aplicar filtros:\n{str(e)}")

    # def clear_filters(self):
    #     """Limpia todos los filtros aplicados"""
    #     self.ubicacion_filter.set("")
    #     self.sexo_filter.set("")
    #     self.edad_min_entry.delete(0, tk.END)
    #     self.edad_max_entry.delete(0, tk.END)
        
    #     if self.current_results is not None:
    #         self.show_results(self.current_results)

    def clear_filters(self):
        """Limpia todos los filtros aplicados y vuelve a los resultados originales"""
        self.ubicacion_filter.set("")
        self.sexo_filter.set("")
        self.edad_min_entry.delete(0, tk.END)
        self.edad_max_entry.delete(0, tk.END)
        
        # Recargar los resultados originales de la última búsqueda
        # if hasattr(self, 'last_search_results'):
        if hasattr(self, 'last_search_results') and self.last_search_results is not None:
            self.current_results = self.last_search_results
            self.show_results(self.current_results)
            self.load_filter_options()  # Recargar opciones de filtro

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
                
                resultados = self.df.filter(
                    (F.upper(F.col("paterno")) == paterno) & 
                    (F.upper(F.col("materno")) == materno) &
                    (F.upper(F.col("nombres")).contains(nombres))
                )
            
            # Mostrar resultados
            count = resultados.count()
            elapsed = time() - start_time
            
            self.results_text.config(state=tk.NORMAL)
            self.results_text.delete(1.0, tk.END)
            self.results_text.insert(tk.END, f"⚡ Búsqueda completada en {elapsed:.2f}s\n")
            self.results_text.insert(tk.END, f"📊 Registros encontrados: {count}\n\n")
            
            if count > 0:
                preview = resultados.limit(100).toPandas()
                self.results_text.insert(tk.END, preview.to_string(index=False))
                self.current_results = resultados
                self.last_search_results = resultados  # Guardar copia de los resultados originales
                # Actualizar filtros inmediatamente después de la búsqueda
                self.load_filter_options()
            else:
                self.current_results = None
                self.last_search_results = None
                
            self.results_text.config(state=tk.DISABLED)
            
        except Exception as e:
            messagebox.showerror("Error", f"Error en la búsqueda:\n{str(e)}")

    def export_results(self):
        """Exporta los resultados a un archivo TXT con timestamp"""
        if self.current_results is None:
            messagebox.showwarning("Advertencia", "No hay resultados para exportar")
            return
            
        try:
            # Crear directorio Results si no existe
            os.makedirs("Results", exist_ok=True)

            # Generar nombre de archivo con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"Results/resultados_{timestamp}.txt"
            
            self.status_var.set("Exportando resultados...")
            self.master.update()
            
            start_time = time()
            
        #     # Exportar directamente a un solo archivo
        #     (self.current_results
        #      .coalesce(1)
        #      .write
        #      .option("header", "true")
        #      .option("delimiter", "|")
        #      .mode("overwrite")
        #      .csv("temp_export"))
            
        #     # Renombrar el archivo de salida
        #     for file in os.listdir("temp_export"):
        #         if file.startswith("part-"):
        #             os.rename(
        #                 os.path.join("temp_export", file),
        #                 output_path
        #             )
        #             break
                
        #     # Eliminar directorio temporal
        #     os.rmdir("temp_export")
            
        #     elapsed = time() - start_time
        #     self.status_var.set(f"✅ Exportado a {output_path} en {elapsed:.2f}s")
        #     messagebox.showinfo("Éxito", f"Resultados exportados a:\n{os.path.abspath(output_path)}")
            
        # except Exception as e:
        #     messagebox.showerror("Error", f"Error al exportar:\n{str(e)}")
        #     self.status_var.set("Error en exportación")

####Ojo aca, esto arregla el error WinError 145
            # Solución robusta para el directorio temporal
            temp_dir = "temp_export_" + str(int(time()))
            (self.current_results
            .coalesce(1)
            .write
            .option("header", "true")
            .option("delimiter", "|")
            .mode("overwrite")
            .csv(temp_dir))
            
            # Encontrar y mover el archivo resultante
            for file in os.listdir(temp_dir):
                if file.startswith("part-"):
                    os.rename(
                        os.path.join(temp_dir, file),
                        output_path
                    )
                    break
            
            # Eliminar directorio temporal de forma segura
            for remaining_file in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, remaining_file))
            os.rmdir(temp_dir)
            
            elapsed = time() - start_time
            self.status_var.set(f"✅ Exportado a {output_path} en {elapsed:.2f}s")
            messagebox.showinfo("Éxito", f"Resultados exportados a:\n{os.path.abspath(output_path)}")
            
        except Exception as e:
            # Limpieza en caso de error
            if 'temp_dir' in locals():
                for file in os.listdir(temp_dir):
                    os.remove(os.path.join(temp_dir, file))
                os.rmdir(temp_dir)
            messagebox.showerror("Error", f"Error al exportar:\n{str(e)}")
            self.status_var.set("Error en exportación")
####hasta aca

    def show_results(self, results_df):
        """Muestra los resultados en el área de texto"""
        count = results_df.count()
        self.current_results = results_df
        
        self.results_text.config(state=tk.NORMAL)
        self.results_text.delete(1.0, tk.END)
        self.results_text.insert(tk.END, f"📊 Registros encontrados: {count}\n\n")
        
        if count > 0:
            preview = results_df.limit(100).toPandas()
            self.results_text.insert(tk.END, preview.to_string(index=False))
            
        self.results_text.config(state=tk.DISABLED)

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