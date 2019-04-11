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

def success(machineID,finishEvent,portFinished,rootIP = "192.168.1.9",rootPort = "5556"):
    
    topic = machineID

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://%s:%s" % (rootIP,rootPort))

    while True:
        finishEvent.wait()
        socket.send_string("%d %s" % (topic, portFinished.value))
        finishEvent.clear()



if __name__ == "__main__": 
    successPort = "7001"
    rootIP = "192.168.1.9"
    def test(finishEvent,portFinished):
        while True:
            time.sleep(2)
            portFinished = "5554"
            finishEvent.set()

    manager = mp.Manager()
    finishEvent = manager.Event()
    portFinished = manager.Value('s',"5556")
    p = mp.Process(target=test,args=(finishEvent,portFinished))
    p.start()
    success(machineID,finishEvent=finishEvent,portFinished=portFinished,rootIP=rootIP,rootPort=successPort)
    p.join()

        
