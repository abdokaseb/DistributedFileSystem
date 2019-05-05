import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector
import os

from trackSlaves import recevHeartBeat as HBSlaves
from HandleClientsToSlave import communicate as CScomm

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from Constants import portsHandleClentsToSlaves, portsdatabaseClients, portsdatabaseSlaves, DatabaseportToListenSlaves
from Util import getMyIP

def test(portsAvailable):
    print("start Test")
    while True:
        print("test portsAvailable form master ",portsAvailable)
        time.sleep(1)

if __name__ == "__main__":
    machineIP = getMyIP()
    print("Start DNS logging with machine IP={}".format(machineIP))
    manager = mp.Manager()

    portsAvailableForSlaves = manager.dict()

    trackSlavesProcess = mp.Process(target=HBSlaves, args=(
        portsAvailableForSlaves, machineIP, DatabaseportToListenSlaves))
    trackSlavesProcess.start()

    slavesIPProcess = mp.Pool(len(portsHandleClentsToSlaves))
    slavesIPProcess.starmap_async(CScomm,[(portsAvailableForSlaves,machineIP,port) for port in portsHandleClentsToSlaves])

    trackSlavesProcess.join()
    slavesIPProcess.join()
