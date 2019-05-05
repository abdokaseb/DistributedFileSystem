import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector
import os
from HandleClients import communicate
from distributor import distributorF
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
    withDNS = int(sys.argv[1])
    print("Start Database master logging with machine IP={}".format(machineIP))
    manager = mp.Manager()
    sqlQueue = manager.Queue()
    distributorProcess = mp.Process(target=distributorF,args=(sqlQueue,portsdatabaseSlaves))
    distributorProcess.start()

    portsAvailableForSlaves = manager.dict()
    # testProcess = mp.Process(target=test,args=(portsAvailableForSlaves,))
    # testProcess.start()

    clientsProcesses = mp.Pool(len(portsdatabaseClients))
    clientsProcesses.starmap_async(communicate,[(port,sqlQueue) for port in portsdatabaseClients])
    
    if withDNS == 1:    
        trackSlavesProcess = mp.Process(target=HBSlaves, args=(
            portsAvailableForSlaves, machineIP, DatabaseportToListenSlaves))
        trackSlavesProcess.start()

        slavesIPProcess = mp.Pool(len(portsHandleClentsToSlaves))
        slavesIPProcess.starmap_async(CScomm,[(portsAvailableForSlaves,machineIP,port) for port in portsHandleClentsToSlaves])

    
    distributorProcess.join()
    clientsProcesses.join()
    slavesIPProcess.join()
    trackSlavesProcess.join()
