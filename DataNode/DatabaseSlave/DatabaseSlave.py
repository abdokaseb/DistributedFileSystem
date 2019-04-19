import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector

from aliveDatabaseSlave import sendHeartBeat
from HandleClients import communicate as CComm
from HandleMaster import communicate as MCom
from Constants import portsDatanodeClient

def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())


if __name__ == "__main__":
    machineIP = getMachineIP()
    machineID = 2
    rootIP = "127.0.0.1"
    portToListenSlaves = "8101"
    portsdatabaseSlaves = ["8001","8002","8003","8004","8005","8006"]
    #portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]
    portsMasterClient = ["5001","5002","5003","5004","5005","5006"]

    aliveProcess = mp.Process(target=sendHeartBeat,args=(1,rootIP,portToListenSlaves))
    aliveProcess.start()

    masterProcess = mp.Process(target=MCom,args=(portsdatabaseSlaves[machineID],rootIP))
    masterProcess.start()

    clientsProcesses = mp.Pool(len(portsDatanodeClient))
    clientsProcesses.starmap_async(CComm,[(port,machineIP) for port in portsDatanodeClient])


    aliveProcess.join()
    masterProcess.join()
    clientsProcesses.join()
