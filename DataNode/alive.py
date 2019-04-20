import zmq
import random
import sys
import time
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Util import getMyIP,getLogger

machineID = int(sys.argv[1])

machineIP = getMyIP()

def sendHeartBeat(machineID,rootIP = "192.168.1.9",port = "5556"):
    getLogger().info("Start to send heart beat, connecting to IP:Port {}:{}".format(rootIP,port))
    topic = machineID

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://%s:%s" % (rootIP,port))

    # Data is published along with a topic. The subscribers usually sets a filter on these topics for topic of their interests.

    while True:
        socket.send_string("%d %s" % (topic, machineIP))
        time.sleep(1)
    getLogger().error("sending heart beat finished")

if __name__ == "__main__": 
    if len(sys.argv)>=3:
        import multiprocessing
        num = int(sys.argv[2])
        p = multiprocessing.Pool(num)
        p.map(sendHeartBeat,range(num))
    else:
        sendHeartBeat(machineID)
