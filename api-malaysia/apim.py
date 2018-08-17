#!/usr/bin/python
import requests
import json
import schedule
import time
requests.packages.urllib3.disable_warnings()

import conf
__author__ = 'Chun-Yeow Yeoh'

def post_telemetry_tb(data):
    try:
       headers = {'charset':'utf-8','Content-Type':'application/json', 'Accept':'text/plain'}
       url = "https://" + conf.url + "/api/v1/" + conf.access_token + "/telemetry"
       post_response = requests.post(url, data, headers=headers, verify=True)
       return post_response
    except requests.exceptions.ConnectionError:
       return

def get_json_data(token):
    try:
       headers = {'charset':'utf-8','Content-Type':'application/json', 'Accept':'application/json', 'Authorization': 'Bearer ' + token }
       url = conf.api_url
       get_resp = requests.get(url, headers=headers, verify=False)
       if get_resp.status_code == 200:
          return get_resp
       else:
          return
    except requests.exceptions.ConnectionError:
       return

def query_scheduler():
    try:
       print("[APIM Sched] Air Pollution Index Scheduler")
       res = get_json_data(conf.token)
       data = res.json()
       api_data = data['24hour_api']
       #time = api_data[0]
       #print time
       d = ""
       for i in range(1,65):
          if not api_data[i][25][:-2] == "" and not api_data[i][25][:-2] == "N":
             d += "\"" + api_data[i][0] + ":" + api_data[i][1] + "\":" + api_data[i][25][:-2] + ","
       d += "\"" + api_data[66][0] + ":" + api_data[66][1] + "\":" + api_data[66][25][:-2] + "}"
       d = "{" + d
       print d
       post_telemetry_tb(d)
       return
    except IndexError:
       print "[APIM Sched] Error"
       return

schedule.every(300).seconds.do(query_scheduler)
#schedule.every().hour.do(query_scheduler)

while True:
    schedule.run_pending()
    time.sleep(1)

