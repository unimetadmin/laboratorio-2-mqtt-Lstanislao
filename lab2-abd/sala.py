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
    # obtener la ultima hora registrada en cvs si existe para no generar distorcion en el grafico
    #puesto que si se volvia a ejecuatr con la hora actual podia quedar atrasado y egenrar un punto atras en la grafica
    try:
        df = pd.read_csv("sala.csv")
        df = df.tail()
        horaBase = datetime.datetime.strptime(
            df.iloc[4].time, '%Y-%m-%d %H:%M:%S')
    except:
        horaBase = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    while(True):
        numPersonas = int(np.random.uniform(0, 10))
        hora = getHora(horaBase)
        horaBase = hora

        if (numPersonas > 5):
            payload = {
                "Contador de personas": str(numPersonas),
                "alerta": "Peligro de contagio",
                "fecha": str(hora)
            }
        else:
            payload = {
                "Contador de personas": str(numPersonas),
                "fecha": str(hora)
            }

        client.publish('casa/sala/contador_personas',
                       json.dumps(payload), qos=0)
        print(payload)

        time.sleep(1)


def getHora(horaBase):
    hora = horaBase + datetime.timedelta(minutes=1)
    return hora


if __name__ == '__main__':
    main()
    sys.exit(0)
