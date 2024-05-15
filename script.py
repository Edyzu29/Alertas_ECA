from algoritmo import *
from scrip_telegram import *
import sys

if __name__ == "__main__":
    # Ejemplo:
    # json_dict =  "{#date#:#2024-04-25$17:00:00#,#CO#:637.25}"
    # codigo_estacion = "CA-CC-01"

    json_dict = str(sys.argv[1])
    codigo_estacion = str(sys.argv[2]) 

    # json_dict =  "{#date#:#2024-04-25$17:00:00#,#CO#:637.25}"
    # codigo_estacion = "CA-CC-01"

    Tabla_img = Graficar_Tabla_Estacion()

    Tabla_img.setup(json_df=json_dict, codigo=codigo_estacion)
    Tabla_img.Graficar()

    valor = Tabla_img.get_value()
    parametro = Tabla_img.get_parametro()
    tiempo = Tabla_img.get_date()
    telegram = Tabla_img.get_telegram()
    data_cruda = Tabla_img.get_data_cruda()
    
    # print(f"valor:{valor} estacion:{codigo_estacion} contami:{parametro} timepo:{tiempo} telgram: {telegram} data: {data_cruda}")

    enviar_img(estacion=codigo_estacion, valor=valor, contaminante=parametro, tiempo=tiempo,
               telegram_comisionado=telegram, data_cruda=data_cruda)