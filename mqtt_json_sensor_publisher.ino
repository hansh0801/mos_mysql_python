#include <Adafruit_BMP183_U.h>

#include "DHT.h"
#include <ArduinoJson.h>
#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <math.h>

#define DHTPIN D4
#define DHTTYPE DHT11

DHT dht(DHTPIN, DHTTYPE);

const char* ssid = "KNB";
const char* password = "medicilab";
const char* mqtt_server = "192.168.0.55";

WiFiClient espClient;
PubSubClient client(espClient);
long lastMsg = 0;
char msg[2000];

void setup() {
  Serial.begin(115200);
  dht.begin();
  setup_wifi();
  client.setServer(mqtt_server, 1883);
  client.setCallback(callback);
}

void setup_wifi() {

  delay(10);
  // We start by connecting to a WiFi network
  Serial.println();
  Serial.print("Connecting to ");
  Serial.println(ssid);

  WiFi.begin(ssid, password);

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println("");
  Serial.println("WiFi connected");
  Serial.println("IP address: ");
  Serial.println(WiFi.localIP());
}

void callback(char* topic, byte* payload, unsigned int length) {
  Serial.print("Message arrived [");
  Serial.print(topic);
  Serial.print("] ");
  for (int i = 0; i < length; i++) {
    Serial.print((char)payload[i]);
  }
  Serial.println();

  // Switch on the LED if an 1 was received as first character
  if ((char)payload[0] == '1') {
    digitalWrite(BUILTIN_LED, LOW);   // Turn the LED on (Note that LOW is the voltage level
    // but actually the LED is on; this is because
    // it is acive low on the ESP-01)
  } else {
    digitalWrite(BUILTIN_LED, HIGH);  // Turn the LED off by making the voltage HIGH
  }

}

void reconnect() {
  // Loop until we're reconnected
  while (!client.connected()) {
    Serial.print("Attempting MQTT connection...");
    // Attempt to connect
    if (client.connect("ESP8266Client")) {
      Serial.println("connected");
      // Once connected, publish an announcement...
      client.publish("outTopic", "hello world");
      // ... and resubscribe
      client.subscribe("inTopic");
    } else {
      Serial.print("failed, rc=");
      Serial.print(client.state());
      Serial.println(" try again in 5 seconds");
      // Wait 5 seconds before retrying
      delay(5000);
    }
  }
}

float makeJson(float temp, float humi) { //함수로 받아온 데이터값을 Json으로 변환 시킵니다.
  StaticJsonBuffer<1000> jsonBuffer;
  JsonObject& root = jsonBuffer.createObject();
  root["ID_farm"] = 1;
  root["Sensor_sort"] = "temphumi";
  root["Temp"] = temp;
  root["Humi"] = humi;

  Serial.print("Json data : ");
  root.printTo(Serial);
  Serial.println();
  root.printTo(msg); //json으로 변환한 데이터를 char msg[] 변수에 넣어줍니다.
}

void loop() {

  float t = dht.readTemperature();
  float h = dht.readHumidity();
 // float hic = dht.computeHeatIndex(t, h, false); 
  //float rounded_hic =((int)(hic * 100 ))/ 100.0; //소수점 둘째 자리까지 출력
  makeJson(t, h);
  
  if (!client.connected()) {
    reconnect();
  }
  client.loop();

  long now = millis();
  if (now - lastMsg > 2000) {
    lastMsg = now;
    Serial.print("Publish message: ");
    Serial.println(msg);
    client.publish("1/temphumi", msg); //Sensor 토픽에 해당 Json데이터를 송신합니다.
  }
  delay(60000);
}

