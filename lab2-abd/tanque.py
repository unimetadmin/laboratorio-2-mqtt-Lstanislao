import datetime
import numpy as np
import paho.mqtt.publish
import paho.mqtt.client
import time
import random
import json
import sys
import ssl


def main():
    client = paho.mqtt.client.Client("Unimet", False)
    client.qos = 0
    client.connect(host='localhost')
    medidor = 100
    cicloLlenado = 1
    # obtener la ultima hora registrada en cvs si existe para no generar distorcion en el grafico
    # puesto que si se volvia a ejecuatr con la hora actual podia quedar atrasado y egenrar un punto atras en la grafica
    try:
        df = pd.read_csv("tanque.csv")
        df = df.tail()
        horaBase = datetime.datetime.strptime(
            df.iloc[4].time, '%Y-%m-%d %H:%M:%S')
    except:
        horaBase = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    while(True):
        medVacio = medidor*0.1
        desTanque = medidor*0.05
        salida = np.random.normal(medVacio, desTanque)
        medidor = medidor - salida
        hora = getHora(horaBase)
        horaBase = hora

        if(medidor < 0):  # como la reduccion del tanque es % nunca llega 0 pero quien sabe
            medidor = 0

        if(cicloLlenado == 3):
            medLlenado = medidor*0.2
            llenado = np.random.normal(medLlenado, desTanque)
            medidor += llenado
            if (medidor > 100):
                medidor = 100
            cicloLlenado = 1
        else:
            cicloLlenado += 1

        if(medidor < 50):
            alertaTanque = "Niveles de menores a 50%"
        elif(medidor <= 0):
            alertaTanque = "No hay agua disponible en el tanque"
        else:
            alertaTanque = ''

        medidor = round(medidor, 2)

        if(alertaTanque != ''):
            payload = {
                "% del Tanque": str(medidor),
                "fecha": str(hora),
                "alerta": alertaTanque
            }
        else:
            payload = {
                "% del Tanque": str(medidor),
                "fecha": str(hora),
            }

        client.publish('casa/baño/nivel_tanque',
                       json.dumps(payload), qos=0)
        print(payload)
        time.sleep(1)


def getHora(horaBase):
    hora = horaBase + datetime.timedelta(minutes=10)
    return hora


if __name__ == '__main__':
    main()
    sys.exit(0)
