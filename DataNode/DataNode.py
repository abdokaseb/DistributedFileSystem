import sys
import zmq
import time
import multiprocessing as mp
import multiprocessing.pool
import mysql.connector

from HandleRequests import communicate
from handleReplica import handleReplica as hp
from alive import sendHeartBeat

from Constants import portsDatanodeClient, portsDatanodeDatanode, masterHeartPort, MASTER_FILESYSTEM_MACHINE_IP

machineID=1


class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess


class NoDaemonPool(multiprocessing.pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NoDaemonPool, self).__init__(*args, **kwargs)


def Test():
    import wmi
    c = wmi.WMI()

    while True :
        for process in c.Win32_Process():
            print (process.ProcessId, process.Name)
        time.sleep(10)

if __name__ == "__main__":

    machineID = int(sys.argv[1])

    DIR = sys.argv[2]

    #### for client and master
    mainProcesses = NoDaemonPool(len(portsDatanodeClient))
    mainProcesses.starmap_async(communicate, [(port, DIR) for port in portsDatanodeClient])
    

    ############
    testProcess = mp.Process(target=Test)
    testProcess.start()
    ############


    #### replica processes
    replicaProcesses = mp.Pool(len(portsDatanodeDatanode))
    replicaProcesses.map_async(hp, portsDatanodeDatanode)

    ################ alive process 

    aliveProcesses = mp.Process(target=sendHeartBeat, args=(
        machineID, MASTER_FILESYSTEM_MACHINE_IP, masterHeartPort))
    aliveProcesses.start()
    ###############################
    mainProcesses.close()
    replicaProcesses.close()
    mainProcesses.join()
    replicaProcesses.join()
