import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector

from HandleClients import communicate
from distributor import distributorF
from trackSlaves import recevHeartBeat as HBSlaves
from HandleClientsToSlave import communicate as CScomm
from Constants import portsHandleClentsToSlaves, portsdatabaseClients

def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())

def test(portsAvailable):
    print("start Test")
    while True:
        print("test portsAvailable form master ",portsAvailable)
        time.sleep(1)

if __name__ == "__main__":
    #portsdatabaseClients = ["7001","7002","7003","7004","7005","7006"]
    portsdatabaseSlaves = ["8001","8002","8003","8004","8005","8006"]
    portToListenSlaves = "8101"
    #portsHandleClentsToSlaves = ["8201","8202","8203","8204","8205","8206"]

    machineIP = getMachineIP()
    manager = mp.Manager()
    sqlQueue = manager.Queue()
    distributorProcess = mp.Process(target=distributorF,args=(sqlQueue,portsdatabaseSlaves))
    distributorProcess.start()

    portsAvailableForSlaves = manager.dict()
    testProcess = mp.Process(target=test,args=(portsAvailableForSlaves,))
    testProcess.start()

    trackSlaves = mp.Process(target=HBSlaves,args=(portsAvailableForSlaves,machineIP,portToListenSlaves))
    trackSlaves.start()

    clientsProcesses = mp.Pool(len(portsdatabaseClients))
    clientsProcesses.starmap_async(communicate,[(port,sqlQueue) for port in portsdatabaseClients])

    slavesIPProcess = mp.Pool(len(portsHandleClentsToSlaves))
    slavesIPProcess.starmap_async(CScomm,[(portsAvailableForSlaves,port) for port in portsHandleClentsToSlaves])

    distributorProcess.join()
    clientsProcesses.join()
    slavesIPProcess.join()
