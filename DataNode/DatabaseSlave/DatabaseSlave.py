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
from Constants import portsDatanodeClient, portsdatabaseSlaves, DatabaseportToListenSlaves, MASTER_DATABASE_MACHINE_IP


def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())


if __name__ == "__main__":
    machineIP = getMachineIP()
    machineID = 2
    #rootIP = "127.0.0.1"

    aliveProcess = mp.Process(target=sendHeartBeat, args=(
        1, MASTER_DATABASE_MACHINE_IP, DatabaseportToListenSlaves))
    aliveProcess.start()

    masterProcess = mp.Process(target=MCom, args=(
        portsdatabaseSlaves[machineID], MASTER_DATABASE_MACHINE_IP))
    masterProcess.start()

    clientsProcesses = mp.Pool(len(portsDatanodeClient))
    clientsProcesses.starmap_async(CComm,[(port,machineIP) for port in portsDatanodeClient])


    aliveProcess.join()
    masterProcess.join()
    clientsProcesses.join()
