import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import datetime
import pandas as pd


def main():
    client = paho.mqtt.client.Client("Unimet", False)
    client.qos = 0
    client.connect(host='localhost')
    hielo = False

    # obtener la ultima hora registrada en cvs si existe para no generar distorcion en el grafico
    #puesto que si se volvia a ejecuatr con la hora actual podia quedar atrasado y egenrar un punto atras en la grafica
    try:
        df = pd.read_csv("nevera.csv")
        df = df.tail()
        horaBase = datetime.datetime.strptime(
            df.iloc[4].time, '%Y-%m-%d %H:%M:%S')
    except:
        horaBase = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    while(True):

        temp = getTemperatura()
        numHielo = getHielo()
        hora = getHora(horaBase)
        horaBase = hora

        if(hielo):
            payload = {
                "temperatura": str(temp),
                "fecha": str(hora),
                "alerta": ("se fabricaron %s cubitos de hielo" % (numHielo))
            }
            hielo = False
        else:
            payload = {
                "temperatura": str(temp),
                "fecha": str(hora)
            }
            hielo = True

        client.publish('casa/cocina/temperatura_nevera',
                       json.dumps(payload), qos=0)
        print(payload)
        time.sleep(1)


def getTemperatura():
    medNevera = 10
    desNevera = 2
    while(True):
        temp = round(np.random.normal(medNevera, desNevera), 2)
        if(temp > 8 and temp < 12):
            break
    return temp


def getHielo():
    return int(np.random.uniform(0, 10))


def getHora(horaBase):
    hora = horaBase + datetime.timedelta(minutes=5)
    return hora


if __name__ == '__main__':
    main()
    sys.exit(0)
