import paho.mqtt.client as paho
import time
broker="18.182.191.244"
msg =[]

for i in range(1,200):
    temp = "test"
    temp=temp+str(i)
    msg.append(temp)

def on_publish(client,userdata,result):             #create function for callback
    print("data published \n")
    pass
client1= paho.Client("test")                           #create client object
client1.on_publish = on_publish                          #assign function to callback
client1.connect(broker)
test = "test"
i = 0
while True:
                               #establish connection
    client1.publish(msg[i],"test for rasp")
    print(msg[i])
    i+=1
    if i == 199:
        i = 0
    time.sleep(0.001)     #publish
