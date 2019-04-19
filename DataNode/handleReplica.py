import sys
import zmq
import json
import multiprocessing as mp
import time
import sys
sys.path.insert(0,"../DataNode/")
sys.path.insert(0,"../Client/")
sys.path.insert(0,"../MsterTracker/")
from HandleRequests import uploadFile as uploadDst
from AccessFS import Upload as uploadSrc
from Util import getMyIP


def handleReplica(port):
    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % port)
        print(" in replica process with port " + str(port))
        if(socket.recv_string()=="READY"):
            socket.send_string("YES")
            recvMsg = socket.recv_json()
            socket.setsockopt(zmq.LINGER, 0)  #clear socket buffer
            socket.close()
            print(recvMsg)
            if(recvMsg['src']==True):
                print("i will send replica")
                context = zmq.Context()
                successSocket = context.socket(zmq.REQ)
                successSocket.connect("tcp://%s:%s" % tuple(recvMsg['confirmSuccesOnIpPort']))
                try:
                    #mach = '../node_1/' if recvMsg['userID'] == 10 else '../node_2/'
                    uploadSrc((getMyIP(),port),str(recvMsg['userID']) +'_'+recvMsg['fileName'])          
                    successSocket.send_string("success")
                except Exception as e:
                    print("Upload failed on src machine")
                    print(e)
                successSocket.setsockopt(zmq.LINGER, 0)
                successSocket.close()
            elif (recvMsg['src']==False):
                #mach = '../node_2/' if recvMsg['userID'] == 10 else '../node_1/'
                print("i will recv replica")
                try:
                    print(mach+str(recvMsg['userID']) +'_'+recvMsg['fileName'])
                    uploadDst(tuple(recvMsg['recvFromIpPort']),str(recvMsg['userID']) +'_'+recvMsg['fileName'])  
                except Exception as e:
                    print("Upload failed on dst machine")
                    print(e)
        else:
            socket.close()
            
        
    
#python handleReplica.py 3331,2221,8881,5551,1111
if __name__ == "__main__":
    ports = sys.argv[1].split(',')
    replicaProcesses = mp.Pool(len(ports))
    replicaProcesses.map(handleReplica,[port for port in ports])
