import sys
import zmq
import json
import multiprocessing as mp
import time



def handleReplica(port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)
    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % port)

        recvMsg = socket.recv_json()
        if(recvMsg['src']==True):
            print("i will send replica")
            
            time.sleep(6)

            socket = context.socket(zmq.REQ)
            socket.connect("tcp://%s:%s" % recvMsg['src'])
            socket.send("seucess")

        else:
            print("i will recv replica")
            

        socket.close()
        
    
#python handleReplica.py 3331,2221,8881,5551,1111
if __name__ == "__main__":
    ports = sys.argv[1].split(',')
    replicaProcesses = mp.Pool(len(ports))
    replicaProcesses.map(handleReplica,[port for port in ports])
