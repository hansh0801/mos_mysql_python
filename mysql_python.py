# -*- coding: utf-8 -*-
import time
import paho.mqtt.client as paho
import json
import pymysql

broker="broker server" #우리 서버 브로커 ip
recvData = "" # 받는 데이터 없게 초기화
conn = pymysql.connect(host='localhost', user='userid', password='passwd',
                       db='db', charset='utf8') #mysql 에 연결
curs = conn.cursor()
fetch_curs = conn.cursor(pymysql.cursors.DictCursor) #dick형으로 fetch 하기위한 새로운 커서
def fetch_sensors(): # 센서 데이터 목록을 불러옴
    fetch_sensors = "select * from Sensors"
    fetch_curs.execute(fetch_sensors)

    rows = fetch_curs.fetchall()
    for row in rows:
        sub_topic = str(row['ID_farm'])+"/"+row['Sensor_sort']
        client.subscribe((sub_topic,0)) #토픽 구독함
        print("subscribing "+ sub_topic)
    #print("subscribing Sensor1 ")
    #client.subscribe(("Sensor1",0))#Sensor 토픽을 구독해 줍니다.123123
    #print("subscribing Sensor2 ")
    #client.subscribe(("Sensor2",0))
def push_mysql(ID_farm,Sensor_sort,Sensor_data): # 일반 센서의 데이터를 삽입하기 위한 함수
    add_Sensordata = ("INSERT INTO Farm "
               "(ID_farm,Sensor_sort,Sensor_data, Savetime) "
               "VALUES (%s,%s,%s,now())")
    curs.execute(add_Sensordata, (ID_farm),(Sensor_sort),(Sensor_data))
    conn.commit() # 쿼리문 실행 후 커밋 해줘야 함
    print(Sensor_data +" is inserted to mysql")



def push_mysql_temphumi(ID_farm,Sensor_sort,temp,humi): #온습도 같이 2개 있는 센서 데이터를 삽입하기 위한 함수

    add_temp = ("INSERT INTO Farm "
               "(ID_farm,Sensor_sort,Sensor_data, Savetime) "
               "VALUES (%s,%s,%s,now())")
    add_humi = ("INSERT INTO Farm "
               "(ID_farm,Sensor_sort,Sensor_data, Savetime) "
               "VALUES (%s,%s,%s,now())")
    curs.execute(add_temp, (ID_farm,'temp',temp))
    curs.execute(add_humi,(ID_farm,'humi',humi))
    conn.commit() # 쿼리문 실행 후 커밋 해줘야 함
    print("temp/humi is inserted to mysql")
    #conn.close() # 이걸 삽입시 다음께 실행 되지 않으니 특정 상황에 분기되도록 내가 따로 코딩
    #time.sleep(5)
#define callback
def on_message(client, userdata, message):
    #time.sleep(1) # 메세지 하나만 받게 우선 1초로 설정
    recvData = str(message.payload.decode("utf-8"))
    #recvData.split(',')
    #print("received message =  "+ recvData)
    print("topic now receiving message : " + message.topic)

    jsonData = json.loads(recvData) #json 데이터를 dict형으로 파싱
    #print(str(jsonData["Sensor_sort"]))
    if str(jsonData["Sensor_sort"])=="temphumi": #dht11센서일 경우
        ID_farm=jsonData["ID_farm"]
        Sensor_sort=jsonData["Sensor_sort"]
        temp=jsonData["Temp"]
        humi=jsonData["Humi"]
        print("Temprature : " + str(jsonData["Temp"]))
        print("Humiditiy : " + str(jsonData["Humi"]))
        print("insert to mysql")
        push_mysql_temphumi(ID_farm,Sensor_sort,temp,humi)
    else: #다른 경우일 경우
        ID_farm=jsonData["ID_farm"]
        Sensor_sort=jsonData["Sensor_sort"]
        Sensor_data=jsonData["Sensor_data"]
        print("insert to mysql")
        push_mysql(ID_farm,Sensor_sort,Sensor_data)

client = paho.Client() # mqtt 클라이언트 객체 생성
client.on_message=on_message # 클라이언트 객체의 메세지 받는 것 객체 함수에 정의


while True:
    print("connecting to broker ",broker)
    client.connect(broker)#브로커에 connect
     #start loop to process received messages
    fetch_sensors()
    client.loop_forever()
    #time.sleep(2) #딜레이를 약간 주는데 그 이유는 모르겠음 테스트 해봐야 함
    #client.loop_forever()
    #client.disconnect() #disconnect # 메세지를 수신 한 후 토픽 구독 취소
    #client.loop_stop() #stop loop
    #time.sleep(1)  #기다리는 시간임 얼마나 기다릴지 초로 정하면 됨
#git test
