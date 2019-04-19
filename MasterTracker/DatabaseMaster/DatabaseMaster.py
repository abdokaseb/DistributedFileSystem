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

def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())

def test(portsAvailable):
    print("start Test")
    while True:
        print("test portsAvailable form master ",portsAvailable)
        time.sleep(1)

if __name__ == "__main__":

    machineIP = getMachineIP()
    manager = mp.Manager()
    sqlQueue = manager.Queue()
    distributorProcess = mp.Process(target=distributorF,args=(sqlQueue,portsdatabaseSlaves))
    distributorProcess.start()

    portsAvailableForSlaves = manager.dict()
    testProcess = mp.Process(target=test,args=(portsAvailableForSlaves,))
    testProcess.start()

    trackSlaves = mp.Process(target=HBSlaves, args=(
        portsAvailableForSlaves, machineIP, DatabaseportToListenSlaves))
    trackSlaves.start()

    clientsProcesses = mp.Pool(len(portsdatabaseClients))
    clientsProcesses.starmap_async(communicate,[(port,sqlQueue) for port in portsdatabaseClients])

    slavesIPProcess = mp.Pool(len(portsHandleClentsToSlaves))
    slavesIPProcess.starmap_async(CScomm,[(portsAvailableForSlaves,port) for port in portsHandleClentsToSlaves])

    distributorProcess.join()
    clientsProcesses.join()
    slavesIPProcess.join()
