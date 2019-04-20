import zmq
import random
import sys
import time

from Util import getLogger

def sendHeartBeat(machineID,rootIP = "127.0.0.1",port = "5556",machineIP=''):
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
    # if len(sys.argv)>=3:
    #     import multiprocessing
    #     num = int(sys.argv[2])
    #     p = multiprocessing.Pool(num)
    #     p.map(sendHeartBeat,range(num))
    # else:
    #     sendHeartBeat(machineID)
    sendHeartBeat(1,"127.0.0.1",port="8101")