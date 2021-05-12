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
    ollaOn = True
    #obtener la ultima hora registrada en cvs si existe para no generar distorcion en el grafico
    #puesto que si se volvia a ejecuatr con la hora actual podia quedar atrasado y egenrar un punto atras en la grafica
    try:
        df = pd.read_csv("olla.csv")
        df = df.tail()
        horaBase = datetime.datetime.strptime(
            df.iloc[4].time, '%Y-%m-%d %H:%M:%S')
    except:
        horaBase = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    while(ollaOn):

        temp = round(np.random.uniform(0, 150), 2)
        hora = getHora(horaBase)
        horaBase = hora

        if(temp >= 100):
            payload = {
                "temperatura": str(temp),
                "fecha": str(hora),
                "alerta": "El agua ya hirvio"}
            #ollaOn = False
        else:
            payload = {
                "temperatura": str(temp),
                "fecha": str(hora), }

        client.publish('casa/cocina/temperatura_olla',
                       json.dumps(payload), qos=0)
        print(payload)
        time.sleep(1)


def getHora(horaBase):
    hora = horaBase + datetime.timedelta(seconds=1)
    return hora


if __name__ == '__main__':
    main()
    sys.exit(0)
