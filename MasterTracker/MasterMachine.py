import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector

from aliveThreads import recevHeartBeat
from HandleDataNode import success as DNSuccess
from HandleClients import communicate as CComm


def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())

def test(portsAvailable):
    print("start Test")
    while True:
        print("here")
        print(portsAvailable)
        time.sleep(1)

if __name__ == "__main__":
    machineIP = getMachineIP()
    heartPort = "5556"
    successPort = "7001"
    portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]
    portsMasterClient = ["5001","5002","5003","5004","5005","5006"]

    portsAvailable = mp.Manager().dict()
    testProcess = mp.Process(target=test,args=(portsAvailable,))
    testProcess.start()

    aliveProcess = mp.Process(target=recevHeartBeat,args=(portsAvailable,machineIP,heartPort))
    aliveProcess.start()


    successProcess = mp.Process(target=DNSuccess,args=(portsAvailable,machineIP,successPort))
    successProcess.start()


    clientsProcesses = mp.Pool(len(portsMasterClient))
    clientsProcesses.starmap(CComm,[(portsAvailable,port) for port in portsMasterClient])


    aliveProcess.join()
    successProcess.join()
    clientsProcesses.join()
