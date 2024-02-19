

import sys
import yaml
import os
import time
import glob
import shutil
from datetime import date, timedelta, datetime
from mintsXU4 import mintsDefinitions as mD
from mintsXU4 import mintsLatest as mL

import csv
from collections import OrderedDict
import json
import os


# mintsDefinitions         = yaml.load(open("mintsDefinitions.yaml"))
# print(mintsDefinitions)

nodeIDs            = mD.nodeIDs
dataFolder         = mD.dataFolder
latestFolder       = mD.latestFolder
startDate          = mD.startDate
endDate            = mD.endDate


print()
print("MINTS")
print()
 


delta     = timedelta(days=1)

if __name__ == "__main__":
    for nodeID in nodeIDs:
        print("========================NODES========================")
        print("Syncing node data for node "+ nodeID)
        currentDate = startDate
        includeStatements = " "
        
        while currentDate <= endDate:
            print("========================DATES========================")
            currentDateStr = currentDate.strftime("%Y_%m_%d")
            currentDate   += delta
            print(currentDateStr)
            # Only limiting it to dates and node IDs - Not filtering sensor IDs
            # for sensorID in sensorIDs:
            #     print("========================SENSORS========================")

            print("Syncing data from node " + nodeID + " for the date of " + currentDateStr)
            # print(dataFolder)
            csvDataFiles = glob.glob(dataFolder+"/"+nodeID+ "/*/*/*/*"+ currentDateStr+"*.csv")
            csvDataFiles.sort()
            print(csvDataFiles)
            for csvFile in csvDataFiles:
                print("================================================")
                print(csvFile)
                rowList = []
                with open(csvFile, "r") as f:
                    reader = csv.DictReader(f)
                    try:
                        for row in reader:
                            # Check if some of the values in the row are null
                            if not any(value is None for value in row.values()):
                                rowList.append(row)
                    except csv.Error as e:
                        print(f"CSV Error: {e}")

                    sensorID          = csvFile.split("_")[-4]
                    latestDateTime    = mL.readLatestTime(nodeID,sensorID)

                    try:
                        csvLatestDateTime = datetime.strptime(rowList[-1]['dateTime'],'%Y-%m-%d %H:%M:%S.%f')
                        # print(csvLatestDateTime)
                    except Exception as e:
                        print(e)
                        print("Data row not published") 
                        continue
                    print(latestDateTime)
                    if csvLatestDateTime > latestDateTime:
                        for rowData in rowList:
                            try:
                                dateTimeRow = datetime.strptime(rowData['dateTime'],'%Y-%m-%d %H:%M:%S.%f')
                                # print(rowData)
                                if dateTimeRow > latestDateTime:
                                    print("Publishing MQTT Data ==> Node ID:"+nodeID+ ", Sensor ID:"+ sensorID+ ", Time stamp: "+ str(dateTimeRow))
                                    mL.writeMQTTLatestWithID(nodeID,sensorID,rowData)  
                                    time.sleep(0.001)
                                    
                            except Exception as e:
                                print(e)
                                print("Data row not published")
                                continue

                        mL.writeLatestTime(nodeID,sensorID,csvLatestDateTime)
                        print("================================================")
                        print("Latest Date Time ==> Node:"+ nodeID + ", SensorID:"+ sensorID)
                        print(csvLatestDateTime)
                        print("================================================")
          
