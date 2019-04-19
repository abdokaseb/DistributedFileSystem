import json,zmq,time,os,sys,multiprocessing as mp
sys.path.extend(["DataNode/","Client/","MasterTracker/","./"])
import multiprocessing.pool
import mysql.connector
from HandleRequests import communicate,uploadFile as uploadDst
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alive import sendHeartBeat
from AccessFS import Upload as uploadSrc
from Util import getMyIP,getLogger,setLoggingFile

from Constants import portsDatanodeClient, portsDatanodeDatanode, masterHeartPort, MASTER_FILESYSTEM_MACHINE_IP,defaultAvaliableRepiclaPortsDataNodeDataNode,DIR


machineID=1

def handleReplica(port):
    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://%s:%s" % (getMyIP(),port))
        getLogger().info("A Replica Process alive with ports" + str(port))
        if(socket.recv_string()=="READY"):
            socket.send_string("YES")
            recvMsg = socket.recv_json()
            socket.setsockopt(zmq.LINGER, 0)  #clear socket buffer
            socket.close()
            getLogger().info("RECIVED MESSAGE "+ recvMsg)
            if(recvMsg['src']==True):
                getLogger().info("My ID is {} and I will send replica".format(machineID))
                context = zmq.Context()
                successSocket = context.socket(zmq.REQ)
                successSocket.connect("tcp://%s:%s" % tuple(recvMsg['confirmSuccesOnIpPort']))
                try:
                    #mach = '../node_1/' if recvMsg['userID'] == 10 else '../node_2/'
                    uploadSrc((getMyIP(),port),str(recvMsg['userID']) +'_'+recvMsg['fileName'])          
                    successSocket.send_string("success")
                except Exception as e:
                    getLogger().info("My ID is {} and Upload failed ".format(machineID) + str(e))
                successSocket.setsockopt(zmq.LINGER, 0)
                successSocket.close()
            elif (recvMsg['src']==False):
                #mach = '../node_2/' if recvMsg['userID'] == 10 else '../node_1/'
                getLogger().info("My ID is {} and I will recv replica".format(machineID) + str(recvMsg['userID']) +'_'+recvMsg['fileName'])
                try:
                    uploadDst(tuple(recvMsg['recvFromIpPort']),str(recvMsg['userID']) +'_'+recvMsg['fileName'])  
                except Exception as e:
                    getLogger().info("My ID is {} and Upload failed on dst machine ".format(machineID) + str(e))
        else:
            socket.close()
    

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


# def Test():
#     import wmi
#     c = wmi.WMI()

#     while True :
#         for process in c.Win32_Process():
#             print (process.ProcessId, process.Name)
#         time.sleep(10)

if __name__ == "__main__":
    setLoggingFile("DataNode.log")
    machineID = int(sys.argv[1])


    #### for client and master
    mainProcesses = NoDaemonPool(len(portsDatanodeClient))
    mainProcesses.starmap_async(communicate, [(port, DIR) for port in portsDatanodeClient])
    

    ############
    #testProcess = mp.Process(target=Test)
    #testProcess.start()
    ############


    #### replica processes
    handleReplicaProcesses = mp.Pool(len(defaultAvaliableRepiclaPortsDataNodeDataNode))
    handleReplicaProcesses.map_async(handleReplica,defaultAvaliableRepiclaPortsDataNodeDataNode)

    ################ alive process 
    aliveProcesses = mp.Process(target=sendHeartBeat, args=(
        machineID, MASTER_FILESYSTEM_MACHINE_IP, masterHeartPort))
    aliveProcesses.start()
    ###############################
    mainProcesses.close()
    mainProcesses.join()
    handleReplicaProcesses.close()
    handleReplicaProcesses.join()
