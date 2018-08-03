#!/usr/bin/python
import paho.mqtt.client as mqtt
import json
import requests
import conf
from pymongo import MongoClient
import pymongo
__author__ = 'Chun-Yeow Yeoh'
__copyright__ = "Copyright (C) Chun-Yeow Yeoh"

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('v1/devices/me/rpc/request/+')
    #client.publish('v1/devices/me/attributes', get_led_status(), 1)

def insert_db(db, imsi, data): 
    db.nbiot_tb.insert({'imsi':imsi, 'data':data, 'send':'no'})
    return

def get_device_tb(imsi, key):
    try:
       url = conf.url + conf.Access_Token[imsi] + "/attributes?clientKeys=" + key
       get_response = requests.get(url, verify=False)
       return get_response
    except requests.exceptions.ConnectionError:
       return

def day_scheduler(Day):
    return {
       'Monday': '01',
       'Tuesday': '02',
       'Wednesday': '03',
       'Thursday' : '04',
       'Friday' : '05',
       'Saturday' : '06',
       'Sunday' : '07',
    }.get(Day, '08')

def on_message(client, userdata, msg):
    cli = MongoClient()
    cli = MongoClient('localhost', 27017)
    db = cli.nbiot_tb
    print "Topic: " + msg.topic + "\nMessage: " + str(msg.payload)
    data = json.loads(msg.payload)
    if data['method'] == "setLED":
       if data['params'] == True:
          led_status = "0101"
          pubattr = {'LED':'ON'}
       else:
          led_status = "0100"
          pubattr = {'LED':'OFF'}
       insert_db(db, conf.imsi[0], led_status)
       client.publish('v1/devices/me/attributes', json.dumps(pubattr), 1)
    elif data['method'] == "getLED":
       get_response = get_device_tb(conf.imsi[0], "LED")
       if get_response != None:
          cattr = get_response.json()
          requestID = msg.topic.split('/')[5]
          if cattr['LED'] == "ON":
             pubattr = 1
          else:
             pubattr = 0
          client.publish('v1/devices/me/rpc/response/' + requestID, pubattr, 1)
    elif data['method'] == "setDimming":
       bdata = int(float(data['params']))
       brightness_status = format(bdata, 'x').zfill(2)
       brightness_status = "02" + brightness_status
       pubattr = "{\"Brightness\":%d" %bdata + "}"
       insert_db(db, conf.imsi[0], brightness_status)
       client.publish('v1/devices/me/attributes', pubattr, 1)
    elif data['method'] == "getDimming":
       get_response = get_device_tb(conf.imsi[0], "Brightness")
       if get_response != None:
          cattr = get_response.json()
          requestID = msg.topic.split('/')[5]
          client.publish('v1/devices/me/rpc/response/' + requestID, cattr['Brightness'], 1)
    elif data['method'] == "setEnergy":
       if data['params'] == True:
          energy_status = "09"
          insert_db(db, conf.imsi[0], energy_status)
#    elif data['method'] == "setScheduler":
#       print "RPC command set scheduler" 
#       datap = json.loads(data['params'])
#       day_set = day_scheduler(datap['Day'])
#       if day_set == '08':
#          return
#       if datap.has_key('Time1'):
#          time1 = (int(datap['Time1'].split(':')[0]) * 60) + int(datap['Time1'].split(':')[1])
#          brightness1 = int(datap['Brightness1'])
#          sched1 = time1 * 16 + brightness1
#          led_sched = "05" + day_set + "%04x" %sched1
#       if datap.has_key('Time2'):
#          time2 = (int(datap['Time2'].split(':')[0]) * 60) + int(datap['Time2'].split(':')[1])
#          brightness2 = int(datap['Brightness2'])
#          sched2 = time2 * 16 + brightness2
#          led_sched = led_sched + "%04x" %sched2
#       if datap.has_key('Time3'):
#          time3 = (int(datap['Time3'].split(':')[0]) * 60) + int(datap['Time3'].split(':')[1])
#          brightness3 = int(datap['Brightness3'])
#          sched3 = time3 * 16 + brightness3
#          led_sched = led_sched + "%04x" %sched3
#       if datap.has_key('Time4'):
#          time4 = (int(datap['Time4'].split(':')[0]) * 60) + int(datap['Time4'].split(':')[1])
#          brightness4 = int(datap['Brightness4'])
#          sched4 = time4 * 16 + brightness4
#          led_sched = led_sched + "%04x" %sched4
#       led_sched = led_sched.upper()
#       url = "https://guestnet-malaysia.orbiwise.com/rest/nodes/" + conf.DevEUI + "/payloads/dl?port=2&confirmed=false"
#       headers = {'charset':'utf-8','Content-Type':'application/x-www-form-urlencoded'}
#       print led_sched
#       led_sched = led_sched.decode("hex").encode("base64")
#       post_response = requests.post(url, data=led_sched, headers=headers, auth=(conf.username, conf.password), verify=False)
#       print led_sched
    else:
       print "RPC command not recognized"

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(conf.Access_Token[conf.imsi[0]])
client.connect("", 1883, 60)

try:
    client.loop_forever()
except KeyboardInterrupt:
    exit(1)
