import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector

from aliveDatabaseSlave import sendHeartBeat
from HandleClients import communicate as CComm
from HandleMaster import communicate as MCom
from Constants import portsDatanodeClient, portsdatabaseSlaves, DatabaseportToListenSlaves

def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())


if __name__ == "__main__":
    machineIP = getMachineIP()
    machineID = 2
    rootIP = "127.0.0.1"
    portsMasterClient = ["5001","5002","5003","5004","5005","5006"]

    aliveProcess = mp.Process(target=sendHeartBeat, args=(
        1, rootIP, DatabaseportToListenSlaves))
    aliveProcess.start()

    masterProcess = mp.Process(target=MCom,args=(portsdatabaseSlaves[machineID],rootIP))
    masterProcess.start()

    clientsProcesses = mp.Pool(len(portsDatanodeClient))
    clientsProcesses.starmap_async(CComm,[(port,machineIP) for port in portsDatanodeClient])


    aliveProcess.join()
    masterProcess.join()
    clientsProcesses.join()
