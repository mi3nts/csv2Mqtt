import json
import serial
from datetime import date, timedelta, datetime
import os
import csv
#import deepdish as dd
import time
import paho.mqtt.client as mqttClient
import yaml
from mintsXU4 import mintsDefinitions as mD
from collections import OrderedDict
import ssl
import shutil

macAddress          = mD.macAddress
dataFolder          = mD.dataFolder
latestFolder        = mD.latestFolder
credentials         = mD.credentials

# FOR MQTT 
connected   = False  # Stores the connection status
broker      = credentials['broker']  
port        = credentials['port']  
mqttUN      = credentials['username']  
mqttPW      = credentials['password'] 
tlsCert     = "/etc/ssl/certs/ca-certificates.crt"  # Put here the path of your TLS cert
mqtt_client = mqttClient.Client()


def readLatestTime(nodeID,sensorID):
    fileName = latestFolder + "/" + nodeID+"_"+sensorID+".json"
    if os.path.isfile(fileName):
        try:    
            with open(fileName, 'r') as f:
                data = json.load(f)
                print(data)
            return datetime.strptime(data['dateTime'],'%Y-%m-%d %H:%M:%S.%f')

        except Exception as e:
            
            print(e)
    else:
        return datetime.strptime("2020-01-01 00:00:00.000000",'%Y-%m-%d %H:%M:%S.%f')

def directoryCheck2(outputPath):
    isFile = os.path.isfile(outputPath)
    if isFile:
        return True
    if outputPath.find(".") > 0:
        directoryIn = os.path.dirname(outputPath)
    else:
        directoryIn = os.path.dirname(outputPath+"/")

    if not os.path.exists(directoryIn):
        print("Creating Folder @:" + directoryIn)
        os.makedirs(directoryIn)
        return False
    return True;




def deleteEmptyFolders(folder_path):
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for dir_name in dirs:
            current_dir = os.path.join(root, dir_name)
            print("=============")
            print(current_dir)
            if not any(os.path.isfile(os.path.join(current_dir, file)) for file in os.listdir(current_dir)):
                print(f"Deleting folder: {current_dir}")
                try:
                    shutil.rmtree(current_dir)
                    print(f"Successfully deleted: {current_dir}")
                except Exception as e:
                    print(f"Error deleting {current_dir}: {e}")

def deleteFoldersWithOnlyDsStore(folder_path):
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for dir_name in dirs:
            current_dir = os.path.join(root, dir_name)
            if all(file == '.DS_Store' for file in os.listdir(current_dir)):
                print(f"Deleting folder with only .DS_Store files: {current_dir}")
                try:
                    shutil.rmtree(current_dir)
                    print(f"Successfully deleted: {current_dir}")
                except Exception as e:
                    print(f"Error deleting {current_dir}: {e}")

def writeLatestTime(nodeID,sensorID,dateTime):
    fileName = latestFolder + "/" + nodeID+"_"+sensorID+".json"
    directoryCheck2(fileName)
    sensorDictionary = OrderedDict([
                ("dateTime"            ,str(dateTime))
                ])
    with open(fileName, "w") as outfile:
        json.dump(sensorDictionary,outfile)

def on_connect(client, userdata, flags, rc):
    global connected  # Use global variable
    if rc == 0:

        print("[INFO] Connected to broker")
        connected = True  # Signal connection
    else:
        print("[INFO] Error, connection failed")


def on_publish(client, userdata, result):
    print("MQTT Published!")


def connect(mqtt_client, mqtt_username, mqtt_password, broker_endpoint, port):
    global connected
    try:
        if not mqtt_client.is_connected():
            print("Reconnecting")
            mqtt_client.username_pw_set(mqtt_username, password=mqtt_password)
            mqtt_client.on_connect = on_connect
            mqtt_client.on_publish = on_publish
            mqtt_client.tls_set(ca_certs=tlsCert, certfile=None,
                              keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
                              tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
            mqtt_client.tls_insecure_set(False)
            mqtt_client.connect(broker_endpoint, port=port)
            mqtt_client.loop_start()

            attempts = 0

            while not connected and attempts < 5:  # Wait for connection
                print(connected)
                print("Attempting to connect...")
                time.sleep(1)
                attempts += 1

        if not connected:
            print("[ERROR] Could not connect to broker")
            return False
	  
    except Exception as e:
        print(e)
        return False
      
    return True


def writeMQTTLatestWithID(hostID,sensorName,sensorDictionary):

    if connect(mqtt_client, mqttUN, mqttPW, broker, port):
        try:
            mqtt_client.publish(hostID+"/"+sensorName,json.dumps(sensorDictionary))

        except Exception as e:
            print("[ERROR] Could not publish data, error: {}".format(e))
    
    return True
    

def writeMQTTLatest(sensorDictionary,sensorName):

    if connect(mqtt_client, mqttUN, mqttPW, broker, port):
        try:
            mqtt_client.publish(macAddress+"/"+sensorName,json.dumps(sensorDictionary))

        except Exception as e:
            print("[ERROR] Could not publish data, error: {}".format(e))
    
    return True
    



def writeJSONLatest(sensorDictionary,sensorName):
    directoryIn  = dataFolder+"/"+macAddress+"/"+sensorName+".json"
    print(directoryIn)
    try:
        with open(directoryIn,'w') as fp:
            json.dump(sensorDictionary, fp)

    except:
        print("Json Data Not Written")


def readJSONLatestAll(sensorName):
    try:
        directoryIn  = dataFolder+"/"+macAddress+"/"+sensorName+".json"
        with open(directoryIn, 'r') as myfile:
            # dataRead=myfile.read()
            dataRead=json.load(myfile)

        time.sleep(0.01)
        return dataRead, True;
    except:
        print("Data Conflict!")
        return "NaN", False