import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector

from aliveThreads import recevHeartBeat
from HandleDataNode import uploadSucess as DNSuccess
from HandleClients import communicate as CComm

from Constants import masterClientPorts, masterHeartPort, portsDatanodeClient, portsDatanodeDatanode, masterPortUploadSucess




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
    print(machineIP)
    machinesIDs = readMachinesIDs()

    portsAvailable = mp.Manager().dict()
    testProcess = mp.Process(target=test,args=(portsAvailable,))
    testProcess.start()

    aliveProcess = mp.Process(target=recevHeartBeat,args=(portsAvailable,machineIP,masterHeartPort,machinesIDs))
    aliveProcess.start()

    successProcess = mp.Process(target=DNSuccess, args=(
        machineIP, masterPortUploadSucess))
    successProcess.start()

    clientsProcesses = mp.Pool(len(masterClientPorts))
    clientsProcesses.starmap(CComm,[(portsAvailable,port) for port in masterClientPorts])


    aliveProcess.join()
    successProcess.join()
    clientsProcesses.join()
