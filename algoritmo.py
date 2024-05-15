import json
from data import *
import matplotlib.pyplot as plt
import pandas as pd

class Graficar_Tabla_Estacion:

    def __init__(self):
        self.info_comisionados = dict()
        self.lista_estaciones = dict()
        self.name_comsio = str()
        #Inicializar
        self.station_code = str()
        self.data = dict()

    def open_json(self):
        with open(ruta_actual+"/data/directory.json", 'r') as file:
            self.info_comisionados = json.load(file)

        with open(ruta_actual+"/data/Stations.json", 'r') as file:
            self.lista_estaciones = json.load(file)

    def setup(self, json_df, codigo):
        self.open_json()
        self.station_code = codigo
        self.name_comsio = self.get_comisioando()
        self.data = json_df
        self.reform()

    def get_comisioando(self):
        return self.lista_estaciones[self.station_code]["Encargado"]

    def reform(self):
        self.data= self.data.replace('#', '"')
        self.data = json.loads(self.data.replace('$' , " "))

    def get_telegram(self):
        return self.info_comisionados[self.name_comsio]["Telegram"]

    def get_correo(self):
        return self.info_comisionados[self.name_comsio]["Correo"]

    def get_numero(self):
        return self.info_comisionados[self.name_comsio]["Telefono"]

    def get_parametro(self):
        parametro = list(self.data.keys())
        return parametro[1]

    def get_value(self):
        valor = self.data[self.get_parametro()]
        return valor

    def get_date(self):
        return self.data["date"]

    def get_data_cruda(self):
        return self.data.copy()
    def generar_imagen_tabla(self):
        # Para 4 filas el largo es 1.4 / para 22 columnas de ancho es 27
        n_columnas = len(self.data.keys())
        df_data = pd.DataFrame([self.data])

        #Tamaño de imagen
        h_img = 2

        fig, ax = plt.subplots(figsize=(6, h_img))
        ax.axis('off')
        self.tabla = ax.table(cellText=df_data.values, colLabels=df_data.columns, loc='upper left', cellLoc='center')

        fig.subplots_adjust(left=-0.0135, bottom=0.0, right=0.768, top=1)
        self.tabla.scale(1.273, 4.4)
        self.tabla.auto_set_font_size(False)
        self.tabla.set_fontsize(19)

        for j in range(n_columnas):
            self.tabla._cells[(0, j)].set_facecolor('lightblue')

        # Guardar la tabla como una imagen
        plt.savefig(img_tabla_path)

    def Graficar(self):
        self.generar_imagen_tabla()