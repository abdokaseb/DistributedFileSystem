import zmq
import random
import sys
import time
import multiprocessing as mp


machineID = int(sys.argv[1])
def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())
machineIP = getMachineIP()

def success(machineID,finishEvent,portFinished,rootIP = "127.0.0.1",rootPort = "5556"):
    
    topic = machineID

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://%s:%s" % (rootIP,rootPort))

    while True:
        finishEvent.wait()
        socket.send_string("%d %s" % (topic, portFinished.value))
        finishEvent.clear()

def uploadSucess(rootIP,portUploadSucess,UserID, mcahID, fileName):
    # portUploadSucess = "7001"
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect ("tcp://%s:%s" % (rootIP, portUploadSucess))
    socket.send_string("{} {} {}".format(UserID, mcahID, fileName))
    _ = socket.recv_string()


if __name__ == "__main__": 
    successPort = "7001"
    rootIP = "127.0.0.1"
    def test(finishEvent,portFinished):
        while True:
            # time.sleep(2)
            portFinished = "5559"
            finishEvent.set()

    manager = mp.Manager()
    finishEvent = manager.Event()
    portFinished = manager.Value('s',"5556")
    p = mp.Process(target=test,args=(finishEvent,portFinished))
    p.start()
    success(machineID,finishEvent=finishEvent,portFinished=portFinished,rootIP=rootIP,rootPort=successPort)
    p.join()

        
