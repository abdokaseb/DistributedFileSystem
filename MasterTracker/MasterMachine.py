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
        print("test portsAvailable form master ",portsAvailable)
        time.sleep(1)

def readMachinesIDs():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select id from machines")
    idsRows = dbcursour.fetchall()
    return  [str(id[0]) for id in idsRows]

if __name__ == "__main__":
    machineIP = getMachineIP()
    heartPort = "5556"
    successPort = "7001"
    portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]
    portsMasterClient = ["5001","5002","5003","5004","5005","5006"]
    machinesIDs = readMachinesIDs()

    portsAvailable = mp.Manager().dict()
    testProcess = mp.Process(target=test,args=(portsAvailable,))
    testProcess.start()

    aliveProcess = mp.Process(target=recevHeartBeat,args=(portsAvailable,machineIP,heartPort,machinesIDs))
    aliveProcess.start()

    successProcess = mp.Process(target=DNSuccess,args=(portsAvailable,machineIP,successPort))
    successProcess.start()

    clientsProcesses = mp.Pool(len(portsMasterClient))
    clientsProcesses.starmap(CComm,[(portsAvailable,port) for port in portsMasterClient])


    aliveProcess.join()
    successProcess.join()
    clientsProcesses.join()
