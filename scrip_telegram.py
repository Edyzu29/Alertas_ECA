import requests
from data import *
import datetime

ruta_log = "log.txt"
lista_comisionados = dict()
lista_estaciones = dict()


def guardar_log(estacion, valor, contaminante, tiempo, code, data_cruda):
    fecha_hora_actual = datetime.datetime.now()

    # Formatear la fecha y hora actual
    fecha_hora_formateada = fecha_hora_actual.strftime("%Y-%B-%d %H:%M:%S")

    msj_log = (f"estacion: {estacion} || valor: {valor} || contaminante: {contaminante} || tiempo: {tiempo} || "
               f"Estado_msj: {code} || Fecha: {fecha_hora_formateada} || Data Cruda: {data_cruda}\n")

    with open(ruta_log, 'a') as archivo:
        archivo.write(msj_log)

def enviar_mensaje(estacion, valor, contaminante, tiempo, telegram_comisionado, data_cruda):

    msj = f"""🚨!!! **ALERTA** !!!🚨\n@{telegram_comisionado}\nLa estación {estacion} emitió una alerta el {tiempo}\nvalor de {valor} μg/m³ en {contaminante}.\n"""

    datos = {"chat_id": chat_id, "text": msj}
    respuesta = requests.post(url_telegram_api, json=datos)

    guardar_log(estacion, valor, contaminante, tiempo, respuesta.status_code, data_cruda)
    

def enviar_img(estacion, valor, contaminante, tiempo, telegram_comisionado, data_cruda):

    msj = f"""🚨!!! **ALERTA** !!!🚨\n@{telegram_comisionado}\nLa estación {estacion} emitió una alerta el {tiempo}\nvalor de {valor} μg/m³ en {contaminante}.\n"""

    with open(img_tabla_path, 'rb') as img_file:
        files = {'photo': img_file}
        json = {'chat_id': chat_id, 'caption': msj}

        respuesta = requests.post(url_photo_telegram_api, files=files, data=json)

    guardar_log(estacion, valor, contaminante, tiempo, respuesta.status_code, data_cruda)