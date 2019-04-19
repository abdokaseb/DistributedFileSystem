import sys
import os
import zmq
import time
import multiprocessing as mp 
import mysql.connector

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))



from aliveDatabaseSlave import sendHeartBeat
from HandleClients import communicate as CComm
from HandleMaster import communicate as MCom
from Constants import portsdatabaseClients, portsdatabaseSlaves, DatabaseportToListenSlaves, MASTER_DATABASE_MACHINE_IP,portsSlavesClient
from Util import getMyIP,setLoggingFile



if __name__ == "__main__":
    setLoggingFile("DatabaseSlave.log")
    machineIP = getMyIP()
    print(machineIP)
    machineID = 2
    #rootIP = "127.0.0.1"

    aliveProcess = mp.Process(target=sendHeartBeat, args=(
        1, MASTER_DATABASE_MACHINE_IP, DatabaseportToListenSlaves,machineIP))
    aliveProcess.start()

    masterProcess = mp.Process(target=MCom, args=(
        portsdatabaseSlaves[machineID], MASTER_DATABASE_MACHINE_IP))
    masterProcess.start()

    clientsProcesses = mp.Pool(len(portsSlavesClient))
    clientsProcesses.starmap_async(CComm,[(port,machineIP) for port in portsSlavesClient])


    aliveProcess.join()
    masterProcess.join()
    clientsProcesses.join()
