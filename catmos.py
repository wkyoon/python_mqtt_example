import paho.mqtt.client as mqtt
import json
import mysql.connector
from mysql.connector import Error

from pymemcache.client import base
#from datetime import datetime
import time

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("catmos/#")

# The callback for when a PUBLISH message is received from the server.
#mosquitto_pub -h localhost -t catmos -m "{\"mac\":\"12:23:34:56\",\"thrhlder\":\"F1\",\"val01\":0.0}"
def on_message(client, userdata, msg):
    #print(msg.topic+" "+str(msg.payload))
    x = str(msg.payload.decode('UTF-8'))
    #print (x)
    y = json.loads(x)
    #print(y["mac"])
    #print(y["thrhlder"])
    #print(y["val01"])
    #temp = int(round(time.time() * 1000)) 
    millis = int(round(time.time() * 1000))
    seconds = int(round(time.time()))*1000
    print(millis)
    print(seconds)
    #print(datetime.now().fromtimestamp)
    mkey = y["mac"]+"_"+str(seconds)
    mvalue = str(y["val01"])+"_"+str(y["thrhlder"])
    
    print('mcache ',mkey,mvalue)
    #print('mvalue ',mvalue)

    mcache_client.set(mkey,mvalue)
    
    sql = "INSERT INTO tbdata(mac, thrhlder, val01) \
       VALUES ('%s', '%d', '%f' )" % \
       (y["mac"], y["thrhlder"],y["val01"])
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Commit your changes in the database
        connection.commit()
        #print ("insert to db")
        
    except:
    # Rollback in case there is any error
        connection.rollback()
        #print("mysql erro")

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='catmostest',
                                         user='admin',
                                         password='wjstjrn00')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)

mcache_client = base.Client(('localhost', 11211))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
