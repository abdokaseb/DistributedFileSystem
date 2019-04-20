import sys
import zmq
import time
import os
import multiprocessing as mp 
import mysql.connector

from aliveThreads import recevHeartBeat
from HandleDataNode import uploadSucess as DNSuccess
from HandleClients import communicate as CComm
from replicaProcces import replicate as replicateFunc

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Constants import masterClientPorts, masterHeartPort, portsDatanodeClient, portsDatanodeDatanode, masterPortUploadSucess
from Util import getMyIP,getLogger,setLoggingFile




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
        database="lookUpData",
        autocommit = True
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select id from machines")
    idsRows = dbcursour.fetchall()
    return  [str(id[0]) for id in idsRows]

if __name__ == "__main__":
    setLoggingFile("MasterTracker.log")
    machineIP = getMyIP()
    getLogger().info("Master Tracker started with machine IP = {}".format(machineIP))
    # print(machineIP)
    machinesIDs = readMachinesIDs()

    manager = mp.Manager()
    portsAvailable = manager.dict()
    testProcess = mp.Process(target=test,args=(portsAvailable,))
    testProcess.start()

    aliveProcess = mp.Process(target=recevHeartBeat,args=(portsAvailable,machineIP,masterHeartPort,machinesIDs))
    aliveProcess.start()

    successProcess = mp.Process(target=DNSuccess, args=(
        machineIP, masterPortUploadSucess))
    successProcess.start()

    portsAvailabeDatanode = manager.dict()
    replicaProcess = mp.Process(target=replicateFunc,args=(portsAvailabeDatanode,))
    replicaProcess.start()

    clientsProcesses = mp.Pool(len(masterClientPorts))
    clientsProcesses.starmap_async(CComm,[(portsAvailable,machineIP,port) for port in masterClientPorts])


    aliveProcess.join()
    successProcess.join()
    clientsProcesses.join()
    replicaProcess.join()
