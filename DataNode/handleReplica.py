import json,zmq,logging,time,sys,multiprocessing as mp
sys.path.extend(["DataNode/","Client/","MasterTracker/"])
from HandleRequests import uploadFile as uploadDst
from AccessFS import Upload as uploadSrc
from replicaUtilities import getMyIP
from DataNode import machineId


logging.basicConfig(filename='logs/DataNodeReplic.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
defaultAvaliableRepiclaPortsDataNodeDataNode = [str(9000+i) for i in range(20)]

def handleReplica(port):
    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % port)
        logging.info("A Replica Process alive with ports" + str(port))
        if(socket.recv_string()=="READY"):
            socket.send_string("YES")
            recvMsg = socket.recv_json()
            socket.setsockopt(zmq.LINGER, 0)  #clear socket buffer
            socket.close()
            logging.info("RECIVED MESSAGE "+ recvMsg)
            if(recvMsg['src']==True):
                logging.info("My ID is {} and I will send replica".format(machineId))
                context = zmq.Context()
                successSocket = context.socket(zmq.REQ)
                successSocket.connect("tcp://%s:%s" % tuple(recvMsg['confirmSuccesOnIpPort']))
                try:
                    #mach = '../node_1/' if recvMsg['userID'] == 10 else '../node_2/'
                    uploadSrc((getMyIP(),port),str(recvMsg['userID']) +'_'+recvMsg['fileName'])          
                    successSocket.send_string("success")
                except Exception as e:
                    logging.info("My ID is {} and Upload failed ".format(machineId) + str(e))
                successSocket.setsockopt(zmq.LINGER, 0)
                successSocket.close()
            elif (recvMsg['src']==False):
                #mach = '../node_2/' if recvMsg['userID'] == 10 else '../node_1/'
                logging.info("My ID is {} and I will recv replica".format(machineId) + str(recvMsg['userID']) +'_'+recvMsg['fileName'])
                try:
                    uploadDst(tuple(recvMsg['recvFromIpPort']),str(recvMsg['userID']) +'_'+recvMsg['fileName'])  
                except Exception as e:
                    logging.info("My ID is {} and Upload failed on dst machine ".format(machineId) + str(e))
        else:
            socket.close()
            


def startHandleReplica():
    handleReplicaProcesses = mp.Pool(len(defaultAvaliableRepiclaPortsDataNodeDataNode))
    handleReplicaProcesses.map(handleReplica,defaultAvaliableRepiclaPortsDataNodeDataNode)
    
if __name__ == "__main__":
    ports = sys.argv[1].split(',')
    replicaProcesses = mp.Pool(len(ports))
    replicaProcesses.map(handleReplica,[port for port in ports])
