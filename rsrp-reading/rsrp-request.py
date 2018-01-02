#!/usr/bin/python
import requests
import base64
import json
from flask import Flask, jsonify
from flask import abort
from flask import request
from flask import url_for
from flask import make_response
from flask import redirect
from flask_httpauth import HTTPBasicAuth
from datetime import datetime
import tzlocal
import datetime
import time
requests.packages.urllib3.disable_warnings()

import conf
__author__ = 'Chun-Yeow Yeoh'
__copyright__ = "Copyright (C) 2017 Chun-Yeow Yeoh"

app = Flask(__name__)

def auth_tb(username, password):
    try:
       headers = {'charset':'utf-8','Content-Type':'application/json', 'Accept':'application/json'}
       url = "https://<domain>:<port>/api/auth/login"
       data = "{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}"
       post_resp = requests.post(url, data, headers=headers, verify=True)
       if post_resp.status_code == 200:
          return post_resp
       else:
          return
    except requests.exceptions.ConnectionError:
       return

def get_timeseries_data(device_id, key, startTs, endTs, interval, agg, limit, token):
    try:
       headers = {'charset':'utf-8','Content-Type':'application/json', 'Accept':'application/json', 'X-Authorization': 'Bearer ' + token }
       url = "https://<domain>:<port>/api/plugins/telemetry/DEVICE/" + device_id + \
             "/values/timeseries?keys=" + key + "&startTs=" + startTs + "&endTs=" + endTs + "&interval=" + interval + "&limit=" + limit + "&agg=" + agg
       get_resp = requests.get(url, headers=headers, verify=False)
       if get_resp.status_code == 200:
          return get_resp
       else:
          return
    except requests.exceptions.ConnectionError:
       return

def get_currentmillis(year, month, day, start):
    if (start == True):
       v = datetime.datetime(int(year), int(month), int(day), 0, 0, 0, 360700)
    else:
       v = datetime.datetime(int(year), int(month), int(day), 23, 59, 59, 360700)
    d = time.mktime(v.timetuple()) * 1000
    return "{0:13.0f}".format(d)

@app.route('/rsrp', methods=['GET'])
def read_rsrp_per_day():
    date = request.args.get('date', default = None, type = str)
    stop = request.args.get('stop', default = None, type = str)
    interval = request.args.get('interval', default = None, type = str)
    agg = request.args.get('agg', default = None, type = str)
    limit = request.args.get('limit', default = None, type = str)
    year,month,day = date.split('-')
    start = str(get_currentmillis(year, month, day, True))
    #print start
    stop = str(get_currentmillis(year, month, day, False))
    #print stop
    interval = str(86400000)
    agg = "NONE"
    #limit = str(288)
    limit = str(400)
    #rsrp = []
    res = auth_tb(conf.username, conf.password)
    if res != None:
       token = res.json()['token']
    for i in range(len(conf.Device_ID)):
       rsrp = []
       res = get_timeseries_data(conf.Device_ID[i], 'rsrp', start, stop, interval, agg, limit, token)
       data = res.json()
       if res != None and data.has_key('rsrp'):
          params = data['rsrp']
          #print params
          for j in range(len(params)):
             rsrp.append(params[j]['value'])
             #unix_ts.append(params[0]['ts'])
          int_rsrp = [ float(x) for x in rsrp ]
          avg_rsrp = (sum(int_rsrp) / len(params))/10
          print "Device ID: " + str(conf.Device_ID[i])
          print "Number of Points (288): %d" %len(params)
          print "Average RSRP per day: %.2f" %avg_rsrp      
    return 'OK'

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
