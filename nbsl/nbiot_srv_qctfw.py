#!/usr/bin/python
import requests
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from pymongo import MongoClient
import pymongo
import time
import binascii

import conf
__author__ = 'Chun-Yeow Yeoh'
__copyright__ = "Copyright (C) 2017 Chun-Yeow Yeoh"

def post_telemetry_tb(imsi, data):
    try:
       headers = {'charset':'utf-8','Content-Type':'application/json', 'Accept':'text/plain'}
       url = conf.url + conf.Access_Token[imsi] + "/telemetry"
       post_response = requests.post(url, data, headers=headers, verify=False)
       return post_response
    except requests.exceptions.ConnectionError:
       return

class UDPhandler(DatagramProtocol):

    def startProtocol(self):
       print "UDP server start"
       client = MongoClient()
       client = MongoClient('localhost', 27017)
       self.db = client.nbiot_tb

    def stopProtocol(self):
       print "UDP server stop"

    def datagramReceived(self, data, (host, port)):
       try:
          print "received %r from %s:%d" % (data, host, port)
          if (ord(data[0]) != 0x05) and (ord(data[0]) != 0x06) and (ord(data[0]) != 0x07) and (ord(data[0]) != 0x08):
             print "Non supported packet identifier"
             return
          else:
             dIMSI = data[1:16]
             if (ord(data[0]) == 0x05):
                print "Read Timer"
                #post_telemetry_tb(data[1:16],)
             elif (ord(data[0]) == 0x06):
                print "Read Scheduler"
                #post_telemetry_tb(data[1:16],)
             elif (ord(data[0]) == 0x07):
                print "Status Update"
                stat_list = data.split(',')
                #print stat_list
                cellid = stat_list[1]
                imei = stat_list[2]
                rsrp = stat_list[3]
                txpwr = stat_list[4]
                totpwr = stat_list[5]
                txtime = stat_list[6]
                rxtime = stat_list[7]
                ecl = stat_list[8]
                snr = stat_list[9]
                earfcn = stat_list[10]
                pci = stat_list[11]
                rsrq = stat_list[12]
                #print "IMSI: " + dIMSI
                #print "CELLID: " + cellid
                #print "IMEI: " + imei
                #print "RSRP: " + rsrp
                #print "TXPWR: " + txpwr
                #print "TOTPWR: " + totpwr
                #print "TXTIME: " + txtime
                #print "RXTIME: " + rxtime
                #print "ECL: " + ecl
                #print "SNR: " + snr 
                #print "EARFCN: " + earfcn
                #print "PCI: " + pci
                #print "RSRQ: " + rsrq
                led = 0b00001111 & ord(stat_list[13])
                if (ord(stat_list[14][0]) == 0x41):
                   bright = 10;
                else:
                   bright = 0b00001111 & ord(stat_list[14][0])
                print "LED: %2d" %led
                print "Brightness: %2d" %bright
                energy = 72057594037927936*ord(stat_list[15][0]) + 281474976710656*ord(stat_list[15][1]) + 1099511627776*ord(stat_list[15][2]) + 4294967296*ord(stat_list[15][3]) \
			 + 16777216*ord(stat_list[15][4]) + 65536*ord(stat_list[15][5]) + 256*ord(stat_list[15][6]) + ord(stat_list[15][7]) 
                tb_msg = "{\"imsi\":\"" + dIMSI + "\", \"cellid\":\"" + cellid + "\", \"imei\":\"" + imei + "\", \"rsrp\":\"" + rsrp + "\", \"txpwr\":\"" + txpwr + "\", \"topwr\":\"" + totpwr + "\", \"txtime\":\"" + txtime + "\", \"rxtime\":\"" + rxtime + "\", \"ecl\":\"" + ecl + "\", \"snr\":\"" + snr + "\", \"earfcn\":\"" + earfcn + "\", \"pci\":\"" + pci + "\", \"rsrq\":\"" + rsrq + "\", \"LED\":%2d" %led + ", \"Brightness\":%2d" %bright + ", \"Energy\":%d" %energy + "}"
                #print tb_msg
                post_telemetry_tb(dIMSI, tb_msg)
             else:
                pass
             print "Trigger DL Data Transmission"
             if dIMSI in conf.imsi:
                #data = self.db.nbiot_tb.find_one({'imsi':imsi})
                sdata = list(self.db.nbiot_tb.find({'imsi':dIMSI}).sort("_id", pymongo.DESCENDING).limit(1))[0]
                print sdata['data']
                if (not sdata) or (sdata['send'] == "yes"):
                   print "No pending data to transmit"
                   return
                else:
                   self.transport.write(binascii.unhexlify(sdata['data']), (host, port))
                   self.db.nbiot_tb.update({'_id':sdata['_id']}, {'send':'yes', 'data':sdata['data'], 'imsi':sdata['imsi']})
       except IndexError:
          pass

    def connectionRefused(self):
       print "Peer is not listening"

reactor.listenUDP(5000, UDPhandler())
reactor.run()
