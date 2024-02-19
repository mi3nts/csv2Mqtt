
from getmac import get_mac_address
import yaml
import serial.tools.list_ports
from datetime import date, timedelta, datetime


def findMacAddress():
    macAddress= get_mac_address(interface="eth0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="en0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="docker0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="enp1s0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="wlan0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    return "xxxxxxxx"


mintsDefinitions         = yaml.load(open('mintsXU4/credentials/mintsDefinitions.yaml'),Loader=yaml.FullLoader)
credentials              = yaml.load(open('mintsXU4/credentials/credentials.yaml'),Loader=yaml.FullLoader)
nodeIDs                  = yaml.load(open('mintsXU4/credentials/nodeIDs.yaml'),Loader=yaml.FullLoader)


nodeIDs                 = mintsDefinitions['nodeIDs']
dataFolder              = mintsDefinitions['dataFolder']
latestFolder            = mintsDefinitions['latestFolder']

startDate               = datetime.strptime(mintsDefinitions['startDate'], '%Y_%m_%d')
endDate                 = datetime.strptime(mintsDefinitions['endDate'], '%Y_%m_%d')

macAddress              = findMacAddress()


if __name__ == "__main__":
    # the following code is for debugging
    # to make sure everything is working run python3 mintsDefinitions.py 
    print("Mac Address          : {0}".format(macAddress))
    print("Data Folder          : {0}".format(dataFolder))
    print("Latest Folder        : {0}".format(latestFolder))
    print("Start Date           : {0}".format(startDate))
    print("End Date             : {0}".format(endDate))        
    
    #-------------------------------------------#
    print("Node IDs :")
    for nodeID in nodeIDs:
        print("\t{0}".format(nodeID))
    #-------------------------------------------#
