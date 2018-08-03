import requests
import schedule
import time
from datetime import datetime, timedelta
import conf
from pymongo import MongoClient
import pymongo

def auth_tb(username, password):
    try:
       headers = {'charset':'utf-8','Content-Type':'application/json', 'Accept':'application/json'}
       url = conf.url + "/api/auth/login"
       data = "{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}"
       post_resp = requests.post(url, data, headers=headers, verify=True)
       if post_resp.status_code == 200:
          return post_resp
       else:
          return
    except requests.exceptions.ConnectionError:
       return

def post_dev_data(token, access_token, data):
    try:
       headers = {'charset':'utf-8','Content-Type':'application/json', 'Accept':'application/json', 'X-Authorization': 'Bearer ' + token }
       url = conf.url + access_token + "/attributes"
       post_resp = requests.post(url, data, headers=headers, verify=False)
       if post_resp.status_code == 200:
          return post_resp
       else:
          return
    except requests.exceptions.ConnectionError:
       return

def report_attribute(access_token, data):
    res = auth_tb(conf.username, conf.password)
    if res != None:
       token = res.json()['token']
    else:
       return
    post_dev_data(token, access_token, data)
    return

def insert_db(db, imsi, data):
    epoch = int(time.time())
    db.nbiot_tb.insert({'imsi':imsi, 'data':data, 'send':'no', 'timestamp':epoch})
    return

def nextTime(time, minutes):
    return (datetime.strptime(time, '%H:%M') + timedelta(minutes=minutes)).time()

def set_led_dimming(db, sdata, time, imsi):
    global lamp_set
    if sdata.has_key('time1'):
       print "Scheduled Time 1: " + sdata['time1']
       print "Dimming Level 1: %d" %sdata['brightness1']
       if time == sdata['time1']:
          print "Set LED"
          if sdata['brightness1'] > 0:
             led_status = "0101"
             pubattr = "{'LED':'ON'}"
          else:
             led_status = "0100"
             pubattr = "{'LED':'OFF'}"
          print led_status
          insert_db(db, imsi, led_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
       if time == nextTime(sdata['time1'], 1).strftime('%H:%M'):
          print "Set Dimming"
          bdata = int(float(sdata['brightness1']))
          brightness_status = format(bdata, 'x').zfill(2)
          brightness_status = "02" + brightness_status
          pubattr = "{\"Brightness\":%d" %bdata + "}"
          print brightness_status
          insert_db(db, imsi, brightness_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
    if sdata.has_key('time2'):
       print "Scheduled Time 2: " + sdata['time2']
       print "Dimming Level 2: %d" %sdata['brightness2']
       if time == sdata['time2']:
          print "Set LED"
          if sdata['brightness2'] > 0:
             led_status = "0101"
             pubattr = "{'LED':'ON'}"
          else:
             led_status = "0100"
             pubattr = "{'LED':'OFF'}"
          print led_status
          insert_db(db, imsi, led_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
       if time == nextTime(sdata['time2'], 1).strftime('%H:%M'):
          print "Set Dimming"
          bdata = int(float(sdata['brightness2']))
          brightness_status = format(bdata, 'x').zfill(2)
          brightness_status = "02" + brightness_status
          pubattr = "{\"Brightness\":%d" %bdata + "}"
          print brightness_status
          insert_db(db, imsi, brightness_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
    if sdata.has_key('time3'):
       print "Scheduled Time 3: " + sdata['time3']
       print "Dimming Level 3: %d" %sdata['brightness3']
       if time == sdata['time3']:
          print "Set LED"
          if sdata['brightness3'] > 0:
             led_status = "0101"
             pubattr = "{'LED':'ON'}"
          else:
             led_status = "0100"
             pubattr = "{'LED':'OFF'}"
          print led_status
          insert_db(db, imsi, led_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
       if time == nextTime(sdata['time3'], 1).strftime('%H:%M'):
          print "Set Dimming"
          bdata = int(float(sdata['brightness3']))
          brightness_status = format(bdata, 'x').zfill(2)
          brightness_status = "02" + brightness_status
          pubattr = "{\"Brightness\":%d" %bdata + "}"
          print brightness_status
          insert_db(db, imsi, brightness_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
    if sdata.has_key('time4'):
       print "Scheduled Time 4: " + sdata['time4']
       print "Dimming Level 4: %d" %sdata['brightness4']
       if time == sdata['time4']:
          print "Set LED"
          if sdata['brightness4'] > 0:
             led_status = "0101"
             pubattr = "{'LED':'ON'}"
          else:
             led_status = "0100"
             pubattr = "{'LED':'OFF'}"
          print led_status
          insert_db(db, imsi, led_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
       if time == nextTime(sdata['time4'], 1).strftime('%H:%M'):
          print "Set Dimming"
          bdata = int(float(sdata['brightness4']))
          brightness_status = format(bdata, 'x').zfill(2)
          brightness_status = "02" + brightness_status
          pubattr = "{\"Brightness\":%d" %bdata + "}"
          print brightness_status
          insert_db(db, imsi, brightness_status)
          report_attribute(conf.Access_Token[imsi], pubattr)
          return
    return

def query_scheduler(imsi):
    print("NB-IoT Street Light Controller Scheduler: " + imsi)
    cli = MongoClient()
    cli = MongoClient('localhost', 27017)
    db = cli.nbiot_tb
    day = datetime.today().weekday()
    time = datetime.now().strftime('%H:%M')
    print "Day: %d" %day
    print "Time: " + time
    sdata = list(db.nbiot_scheduler.find({'imsi':imsi,'day':str(day)}).sort("_id", pymongo.DESCENDING).limit(1))[0]
    set_led_dimming(db, sdata, time, imsi)
    return

schedule.every(20).seconds.do(query_scheduler, conf.imsi[0])
#schedule.every(1).minutes.do(query_scheduler, conf.imsi[0])
#schedule.every().hour.do(job)
#schedule.every().day.at("10:30").do(job)
#schedule.every(5).to(10).minutes.do(job)
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("13:15").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
