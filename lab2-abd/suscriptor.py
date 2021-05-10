import sys
import paho.mqtt.client
import psycopg2
import json
import datetime
import pandas as pd

connection = psycopg2.connect(user="nsjshrrc",
                              password="LoobRLeoa0W92xt1pCL_XWYcTJxuc1T2",
                              host="queenie.db.elephantsql.com",
                              database="nsjshrrc")


def on_connect(client, userdata, flags, rc):
    print('connected (%s)' % client._client_id)
    client.subscribe(topic='casa/#', qos=2)


def on_message(client, userdata, message):
    print('------------------------------')
    print('topic: %s' % message.topic)
    print('payload: %s' % message.payload)
    print('qos: %d' % message.qos)
    data = json.loads(message.payload)
    query(data, message.topic)


def main():
    client = paho.mqtt.client.Client(
        client_id='casa-subs', clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host='127.0.0.1', port=1883)
    client.loop_forever()


def query(data, topic):
    cursor = connection.cursor()
    data['fecha'] = datetime.datetime.strptime(
        data['fecha'], '%Y-%m-%d %H:%M:%S')

    if 'alerta' in data:
        data.pop('alerta')

    if "temperatura_nevera" in topic:
        df2 = pd.DataFrame([data])
        df2.to_csv('nevera.csv', mode='a', header=False)
        query = '''INSERT INTO nevera (temperatura, fecha) VALUES( %s, %s)'''
        cursor.execute(query, (data["temperatura"], data["fecha"]))

    elif "temperatura_olla" in topic:
        df2 = pd.DataFrame([data])
        df2.to_csv('olla.csv', mode='a', header=False)
        query = '''INSERT INTO olla (temperatura, fecha) VALUES( %s, %s)'''
        cursor.execute(query, (data["temperatura"], data["fecha"]))

    elif "contador_personas" in topic:
        df2 = pd.DataFrame([data])
        df2.to_csv('sala.csv', mode='a', header=False)
        query = '''INSERT INTO sala (num_personas, fecha) VALUES( %s, %s)'''
        cursor.execute(
            query, (data["Contador de personas"],  data["fecha"]))

    elif "alexa_echo" in topic:
        df2 = pd.DataFrame([data])
        df2.to_csv('alexa.csv', mode='a', header=False)
        query = '''INSERT INTO alexa (temperatura_ccs, fecha) VALUES( %s, %s)'''
        cursor.execute(query, (data["Temperatura en Caracas"],  data["fecha"]))

    elif "nivel_tanque" in topic:
        df2 = pd.DataFrame([data])
        df2.to_csv('tanque.csv', mode='a', header=False)
        query = '''INSERT INTO tanque (medidor ,fecha) VALUES( %s, %s )'''
        cursor.execute(query, (data["% del Tanque"], data["fecha"]))

    connection.commit()


def createCvs(nombre):
    csvName = str(nombre)+".csv"
    if nombre == 'sala':
        try:
            df = pd.read_csv(csvName)
        except:
            print(("error leyendo %s.cvs. Creando archivo") % (nombre))
            df_create = pd.DataFrame(columns=['contador_personas', 'time'])
            df_create.to_csv(csvName, mode='a', header=True)
    elif nombre == "tanque":
        try:
            df = pd.read_csv(csvName)
        except:
            print(("error leyendo %s.cvs. Creando archivo") % (nombre))
            df_create = pd.DataFrame(columns=['medidor', 'time'])
            df_create.to_csv(csvName, mode='a', header=True)
    else:
        try:
            df = pd.read_csv(csvName)
        except:
            print(("error leyendo %s.cvs. Creando archivo") % (nombre))
            df_create = pd.DataFrame(columns=['temperature', 'time'])
            df_create.to_csv(csvName, mode='a', header=True)


if __name__ == '__main__':
    createCvs("nevera")
    createCvs("olla")
    createCvs("sala")
    createCvs("alexa")
    createCvs("tanque")
    main()


sys.exit(0)
