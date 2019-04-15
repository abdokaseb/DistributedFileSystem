import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector

from HandleClients import communicate
from distributor import distributorF

def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())

if __name__ == "__main__":
    portsdatabaseClients = ["7001","7002","7003","7004","7005","7006"]
    portsdatabaseSlaves = ["8001","8002","8003","8004","8005","8006"]
    
    sqlQueue = mp.Manager().Queue()
    distributorProcess = mp.Process(target=distributorF,args=(sqlQueue,portsdatabaseSlaves))
    distributorProcess.start()


    clientsProcesses = mp.Pool(len(portsdatabaseClients))
    clientsProcesses.starmap(communicate,[(port,sqlQueue) for port in portsdatabaseClients])


    distributorProcess.join()
    clientsProcesses.join()
