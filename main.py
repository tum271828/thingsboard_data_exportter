import os
import math
import time
import glob
import configparser
import subprocess
import shlex
import datetime
import requests
import json
import psycopg2
import pandas as pd
import signal
import paho.mqtt.client as mqtt
from icecream import ic 
from cfg import *
from json2db import *
from textdistance import levenshtein as textdist
import numpy as np 

def dict2(k,v): 
    ans=dict(ts=k)
    ans.update(**v)
    return ans
    
def loadJson(fn,default=None): 
    if os.path.isfile(fn): 
        with open(fn,"r") as fp: 
            return json.load(fp)
    else: 
        if default is None: 
            raise Exception(f"{fn} is not found")
        return default
        
def saveJson(fn,objs,**kargs): 
    with open(fn,"w") as fp: 
        json.dump(objs,fp,**kargs)

def mergeByTs(tbTelemetryData): 
        recs=defaultdict(dict)
        for k,v in data.items(): 
            for obj in v:
                ts,val=obj["ts"],obj["value"]
                recs[ts][k]=val
        return [dict2(k,v) for k,v in recs.items()]

class Thingsboard():

    def __init__(self,user,password,baseUrl):
        self.user=user
        self.password=password
        self.baseUrl=baseUrl
        self.auth()

    def auth(self):
        url = self.baseUrl+'/api/auth/login'
        data = {'username':self.user, 'password':self.password}
        data = json.dumps(data)
        r = requests.post(url,data=data)
        if(r.status_code == 200):
            #ic(r.json())
            self.accessInfo=r.json()
        else :
            raise Exception(r.content)
            
    def accessToken(self):
        return self.accessInfo['token']
            
    def findDevices(self,pattern=None, device_type=None):
        headers = {'X-Authorization': f'Bearer {self.accessToken()}'}
        querys=[]
        if device_type:
            querys.append(f"type={device_type}")
        if pattern:
            querys.append("textSearch={device_name}")
        qstr="&".join(querys)
        url = f"""{self.baseUrl}/api/tenant/devices?pageSize=1000&page=0&{qstr}"""
        r = requests.get(url, headers=headers)
        #ic(url)
        data = r.json()
        #print(data)
        return data["data"]
        
    def getDevCredentials(self,id):
        headers = {'X-Authorization': f'Bearer {self.accessToken()}'}
        if isinstance(id,dict): 
            id=id['id']
        url = f'{self.baseUrl}/api/device/{id}/credentials'
        r = requests.get(url, headers=headers)
        return r.json()
        if data2['credentialsType'] == 'ACCESS_TOKEN':
            return data2['credentialsId']
        else :
            print(f"This device ({device_name}) credentials isn't ACCESS_TOKEN")

    def getLatestTimeSeries(self,id,scope='SHARED_SCOPE',keys='',interval="",limit=100,agg="NONE",orderBy="DESC",typeStrict='false'):
        if isinstance(id,dict): 
            entityType=id['entityType']
            id=id['id']
        headers = {'X-Authorization': f'Bearer {self.accessToken()}'}
        url = f"{self.baseUrl}/api/plugins/telemetry/{entityType}/{id}/values/timeseries?limit={limit}"
        #{interval},{limit},{agg},{orderBy},{typeStrict},{keys},{startTs},{endTs}"
        
        #print(url)  
        #+f'/api/plugins/telemetry/DEVICE/{id}/values/attributes/{scope}'
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            ic(r)
        data = r.json()
        return data
        
    def getTimeSeries(self,id,startTs,endTs,scope='SHARED_SCOPE',keys='',interval="",limit=100,agg="NONE",orderBy="DESC",typeStrict='false'):
        if isinstance(id,dict): 
            entityType=id['entityType']
            id=id['id']
        headers = {'X-Authorization': f'Bearer {self.accessToken()}'}
        url = f"{self.baseUrl}/api/plugins/telemetry/{entityType}/{id}/values/timeseries?startTs={startTs}&endTs={endTs}&keys={keys}&limit={limit}"
        #{interval},{limit},{agg},{orderBy},{typeStrict},{keys},{startTs},{endTs}"
        
        #print(url)  
        #+f'/api/plugins/telemetry/DEVICE/{id}/values/attributes/{scope}'
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            ic(r)
        data = r.json()
        return data
        
    def getAttr(self,id, scope='SHARED_SCOPE'):
        if isinstance(id,dict): 
            entityType=id['entityType']
            id=id['id']
        headers = {'X-Authorization': f'Bearer {self.accessToken()}'}
        url = f'{self.baseUrl}/api/plugins/telemetry/DEVICE/{id}/values/attributes/{scope}'
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            raise Exception(r.content)
        data = r.json()
        return data

tb=Thingsboard(user,password,url)
devices=tb.findDevices()
startTime=loadJson("maxTime.json",datetime.datetime(2022,1,1).timestamp())
timestamp = startTime+1 #time.mktime(time.strptime('2021-11-30 0:0:0', '%Y-%m-%d %H:%M:%S'))
print(datetime.datetime.fromtimestamp(timestamp))

ts=int(timestamp*1000) # end t
timestamp = time.time() #mktime(time.strptime('2025-12-31 0:0:0', '%Y-%m-%d %H:%M:%S'))
ts2=int(timestamp*1000) # end ts


conn = psycopg2.connect(database=DB,host=HOST,user=USER,password=PASSWORD,port=PORT)
driver=PsqlDbDriver(conn,DB)
conv=Json2DbBase(driver,JSONPath("$.devType"),
    timeFieldName=re.compile(".*time"),
    tablePrefix='codonline_') 
    
conv2=Json2DbBase(driver,None,
    timeFieldName=re.compile(".*time"),
    tablePrefix='codonline_',toTable="device") 


def getSite(conn): 
    sql="""SELECT distinct "siteID","sitename" FROM devices"""
    cur=conn.cursor()
    cur.execute(sql)
    rows=cur.fetchall()
    return [[id,name.replace("นิคมอุตสาหกรรม","")] for id,name in rows]

sites=getSite(conn)

def getSiteId(text): 
    text=text.replace("นิคมฯ","")
    dists=[textdist(text,name) for id,name in sites]
    '''inx=np.argmin(dists)
    ic(text,sites[inx])'''
    inx=np.argmin(dists)
    #ic(text,sites[inx])
    return sites[inx][0]
ic(ts,ts2)
allData=[]
headers=[]
for r in devices:
    #ic(r["type"])
    header=dict(device_id=r["name"],point=r["type"])
    for prop in tb.getAttr(r['id'],scope="SERVER_SCOPE"):
        key=prop["key"]
        val=prop["value"]
        if key=="latitude":
            header["lat"]=val 
        if key=="longitude":
            header["long"]=val 
            
    '''ic(tb.getAttr(r['id'],scope="CLIENT_SCOPE"))
    ic(tb.getAttr(r['id'],scope="SERVER_SCOPE"))
    ic(tb.getAttr(r['id'],scope="SHARED_SCOPE"))'''
    
    siteId=getSiteId(r['label'])
    header["siteID"]=siteId
    #cred=tb.getDevCredentials(r['id'])
    headers.append(header)
    data=tb.getLatestTimeSeries(r['id'])
    if len(data): 
        #ic(len(data))
        #ic(r['name'])
        #ic(data)
        keys=",".join(data.keys())
        #keys="COD,BOD,Flow" 
        data=tb.getTimeSeries(r['id'],ts,ts2,keys=keys)
        #ic(len(data))
        #ic(data)
        #ic(ts,ts2)
        #exit()
        fields=dict(device_id=r['name'],devType=r['type'])
        data=mergeByTs(data)
        #ic(data)
        #exit()
        def convert(x): 
            '''x["cod"]=float(x["COD"])
            del x["COD"]
            x["bod"]=float(x["BOD"])
            del x["BOD"]
            x["flow"]=float(x["Flow"])
            del x["Flow"]'''
            val=int(x["ts"])
            if ts<=val<=ts2: 
                x['time']=val//1000
            else: 
                x["time"]=val
            del x["ts"]
            #del x["Battery_Status"]
            return dict(**fields,siteID=siteId,**x)
        data=[convert(x) for x in data]# if ts<=int(x['ts'])<=ts2]
        #ic(data)
        allData.extend(data)
        
        #ds=gen_data.genData(1) 
        #print(ds)
if len(allData):
    #ic(allData)
    maxTs=max([r['time'] for r in allData])
    if 1:
        conv.flatten(allData) 
        conv.scan(allData,insert=0)
        conv.scan(allData)
        conv.done() 
        
        conv2.flatten(headers) 
        conv2.scan(headers)
        conv2.done() 
        saveJson("maxTime.json",maxTs)
    ic(maxTs,datetime.datetime.fromtimestamp(maxTs))
            
"""
def tb_asset_id(access_token, asset_name):
    headers = {'X-Authorization': f'Bearer {access_token}'}
    url = config['thingsboard']['url']+f'/api/tenant/assets?assetName={asset_name}'
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(r.content)
    data = r.json()
    return data['id']['id']

def tb_asset_cctvs(access_token, asset_id):
    headers = {'X-Authorization': f'Bearer {access_token}'}
    url = config['thingsboard']['url']+f'/api/relations/info?fromId={asset_id}&fromType=ASSET'
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(r.content)
    data = r.json()
    cctvs = dict()
    for i, dt in enumerate(data):
        cctv_id = dt['to']['id']
        tok = tb_cctv_token(access_token, cctv_id)
        atr = tb_cctv_attributes(access_token, cctv_id)

        # get latest update ts
        last_update = 0
        attributes = dict()
        for a in atr:
            attributes[a['key']] = a['value']
            if a['lastUpdateTs'] > last_update:
                last_update = a['lastUpdateTs']            

        cctvs[dt['to']['id']] = {'name': dt['toName'], 'access_token': tok, 'attributes':attributes, 'last_update': last_update/1000}
    return cctvs

def tb_cctv_token(access_token, cctv_id):
    headers = {'X-Authorization': f'Bearer {access_token}'}
    url = config['thingsboard']['url']+f'/api/device/{cctv_id}/credentials'
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(r.content)
    data = r.json()
    return data['credentialsId']

def tb_cctv_attributes(access_token, cctv_id, scope='SHARED_SCOPE'):
    headers = {'X-Authorization': f'Bearer {access_token}'}
    url = config['thingsboard']['url']+f'/api/plugins/telemetry/DEVICE/{cctv_id}/values/attributes/{scope}'
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(r.content)
    data = r.json()
    return data
    
def tbTelemetry(access_token, id,startTs,endTs,scope='SHARED_SCOPE',entityType='DEVICE',keys='',interval="",limit=100,agg="NONE",orderBy="DESC",typeStrict='false'):
    headers = {'X-Authorization': f'Bearer {access_token}'}
    url = config['thingsboard']['url']+f"/api/plugins/telemetry/{entityType}/{id}/values/timeseries?startTs={startTs}&endTs={endTs}&keys={keys}&limit={limit}"
    #{interval},{limit},{agg},{orderBy},{typeStrict},{keys},{startTs},{endTs}"
    
    print(url)
    #+f'/api/plugins/telemetry/DEVICE/{id}/values/attributes/{scope}'
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(r.content)
    data = r.json()
    return data
    
def tbSaveAttr(access_token, devId,objs,scope='SHARED_SCOPE',entityType='DEVICE'):
    headers = {'X-Authorization': f'Bearer {access_token}'}
    baseUrl=config['thingsboard']['url']
    url=f"{baseUrl}/api/plugins/telemetry/{entityType}/{devId}/attributes/{scope}"
    
    #url = +f"/api/plugins/telemetry/{entityType}/{id}/values/timeseries?startTs={startTs}&endTs={endTs}&keys={keys}&limit={limit}"
    #{interval},{limit},{agg},{orderBy},{typeStrict},{keys},{startTs},{endTs}"
    
    #print(url)
    #+f'/api/plugins/telemetry/DEVICE/{id}/values/attributes/{scope}'
    r = requests.post(url, headers=headers,json=objs)
    #print(r.content)
    if r.status_code != 200:
        raise Exception(r.content)
        return False 
    return True
    

def tb_server_cctvs():
    access_token = tb_access_token()
    asset_id = tb_asset_id(access_token, config['thingsboard']['asset'])
    return tb_asset_cctvs(access_token, asset_id)

def tb_shared_attr(attr_key, device_token):
    result = requests.get(f"{config['thingsboard']['url']}/api/v1/{device_token}/attributes?sharedKeys={attr_key}")
    result_json = result.json()['shared'][attr_key]
    return result_json

def tb_send_attr(cctv_id, data):
    token = cctv_token(cctv_id)

    client = mqtt.Client()
    client.username_pw_set(token)
    client.connect(config['thingsboard']['mqtt_host'], int(config['thingsboard']['mqtt_port']), 60)
    client.loop_start()
    client.publish('v1/devices/me/attributes', json.dumps(data), 2)
    time.sleep(2)
    client.loop_stop()
    client.disconnect()
    
def tbSendAttr(cctv_id, data):
    token = cctvToken(cctv_id)

    client = mqtt.Client()
    client.username_pw_set(token)
    client.connect(config['thingsboard']['mqtt_host'], int(config['thingsboard']['mqtt_port']), 60)
    client.loop_start()
    client.publish('v1/devices/me/attributes', json.dumps(data), 2)
    time.sleep(2)
    client.loop_stop()
    client.disconnect()
    
def tb_mqtt_conn(cctv_id):
    token = cctv_token(cctv_id)

    client = mqtt.Client()
    client.username_pw_set(token)
    client.connect(config['thingsboard']['mqtt_host'], int(config['thingsboard']['mqtt_port']), 60)
    return client
"""

