# -*- coding: utf-8 -*-
import time
import paho.mqtt.client as paho
import json
import pymysql

broker="192.168.0.55" #브로커서버가 있는 라즈베리파이라서 localhost를 사용합니다.
recvData = ""
conn = pymysql.connect(host='localhost', user='root', password='raspberry',
                       db='Sensor1', charset='utf8')
curs = conn.cursor()

def push_mysql(temp,humi):

    add_temp = ("INSERT INTO temperature "
               "(temperature, savetime) "
               "VALUES (%s, now())")
    add_humi = ("INSERT INTO humidity "
               "(humidity, savetime) "
               "VALUES (%s, now())")
    curs.execute(add_temp, (temp))
    curs.execute(add_humi, (humi))
    conn.commit()
    print("insert complete")
    #conn.close()
    #time.sleep(5)
#define callback
def on_message(client, userdata, message):
    time.sleep(1)
    recvData = str(message.payload.decode("utf-8"))
    print("received message =", recvData)
    jsonData = json.loads(recvData) #json 데이터를 dict형으로 파싱
    temp=jsonData["Temp"]
    humi=jsonData["Humi"]
    print("Temprature : " + str(jsonData["Temp"]))
    print("Humiditiy : " + str(jsonData["Humi"]))
    print("insert to mysql")
    push_mysql(temp,humi)

client = paho.Client()
client.on_message=on_message


while True:
    print("connecting to broker ",broker)
    client.connect(broker)#connect
    client.loop_start() #start loop to process received messages
    print("subscribing ")
    client.subscribe("Sensor1")#Sensor 토픽을 구독해 줍니다.
    time.sleep(2)

    client.disconnect() #disconnect
    client.loop_stop() #stop loop
    time.sleep(60)  #기다리는 시간임 얼마나 기다릴지 초로 정하면 됨
