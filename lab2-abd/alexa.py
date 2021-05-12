import datetime
import numpy as np
import paho.mqtt.publish
import paho.mqtt.client
import time
import random
import json
import sys
import ssl
import urllib3


def main():
    client = paho.mqtt.client.Client("Unimet", False)
    client.qos = 0
    client.connect(host='localhost')
    temp = getTemp()
    # obtener la ultima hora registrada en cvs si existe para no generar distorcion en el grafico
    # puesto que si se volvia a ejecuatr con la hora actual podia quedar atrasado y egenrar un punto atras en la grafica
    try:
        df = pd.read_csv("alexa.csv")
        df = df.tail()
        horaBase = datetime.datetime.strptime(
            df.iloc[4].time, '%Y-%m-%d %H:%M:%S')
    except:
        horaBase = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    while(True):
        hora = getHora(horaBase)
        horaBase = hora
        payload = {
            "Temperatura en Caracas": str(temp),
            "fecha": str(hora)
        }

        client.publish('casa/sala/alexa_echo',
                       json.dumps(payload), qos=0)
        print(payload)
        time.sleep(1)


def getHora(horaBase):
    hora = horaBase + datetime.timedelta(minutes=30)
    return hora


def getTemp():
    http = urllib3.PoolManager()
    r = http.request(
        'GET', 'https://api.weatherbit.io/v2.0/current?&city=Caracas&key=221c8e5225f340ad8a05b01b5bb732a8')
    data = r.data
    data = json.loads(data)
    temp = data['data'][0]['app_temp']
    return temp


if __name__ == '__main__':
    main()
    sys.exit(0)
