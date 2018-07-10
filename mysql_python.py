# -*- coding: utf-8 -*-
import time
import paho.mqtt.client as paho
import json
import pymysql

broker="192.168.0.55" #우리 서버 브로커 ip
recvData = "" # 받는 데이터 없게 초기화
conn = pymysql.connect(host='localhost', user='root', password='raspberry',
                       db='Sensor1', charset='utf8') #mysql 에 연결
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
    conn.commit() # 쿼리문 실행 후 커밋 해줘야 함
    print("insert complete")
    #conn.close() # 이걸 삽입시 다음께 실행 되지 않으니 특정 상황에 분기되도록 내가 따로 코딩
    #time.sleep(5)
#define callback
def on_message(client, userdata, message):
    time.sleep(1) # 메세지 하나만 받게 우선 1초로 설정
    recvData = str(message.payload.decode("utf-8"))
    print("received message =", recvData)
    jsonData = json.loads(recvData) #json 데이터를 dict형으로 파싱
    temp=jsonData["Temp"]
    humi=jsonData["Humi"]
    print("Temprature : " + str(jsonData["Temp"]))
    print("Humiditiy : " + str(jsonData["Humi"]))
    print("insert to mysql")
    push_mysql(temp,humi)

client = paho.Client() # mqtt 클라이언트 객체 생성
client.on_message=on_message # 클라이언트 객체의 메세지 받는 것 객체 함수에 정의


while True:
    print("connecting to broker ",broker)
    client.connect(broker)#브로커에 connect
    client.loop_start() #start loop to process received messages
    print("subscribing ")
    client.subscribe("Sensor1")#Sensor 토픽을 구독해 줍니다.
    time.sleep(2) #딜레이를 약간 주는데 그 이유는 모르겠음 테스트 해봐야 함

    client.disconnect() #disconnect # 메세지를 수신 한 후 토픽 구독 취소
    client.loop_stop() #stop loop
    time.sleep(60)  #기다리는 시간임 얼마나 기다릴지 초로 정하면 됨
