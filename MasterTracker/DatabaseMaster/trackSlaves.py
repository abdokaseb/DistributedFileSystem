import sys
import zmq
import time
import threading 
import mysql.connector
import copy
import os
import logging
logging.basicConfig(level="INFO",filename='logs/aliveDatabaseMasterFromSlaves.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from Constants import portsSlavesClient, N_DATABASE_SLAVES
from Util import getMyIP

machineIP = getMyIP()

def checkLive(portsAvailable,timeStam):
    staticTimeStamp = {}
    Alive = {}

    while True:
        time.sleep(1)
        for recvIP, value in timeStam.items():
            try:
                s = staticTimeStamp[recvIP]
                if (s[0] >= value):
                    s[1] += 1
                    if s[1] > 2 and Alive[recvIP] != 0:
                        Alive[recvIP] = 0
                        logging.info("Database Slaves with IP={} dead".format(recvIP))
                        print("Database Slaves with IP={} dead".format(recvIP))
                        portsAvailable[recvIP] = []
                        
                        
                else:
                    if Alive[recvIP] == 0:
                        logging.info("Database Slaves with and IP={} is alive".format(recvIP))
                        print("Database Slaves with and IP={} is alive".format(recvIP))
                        Alive[recvIP] = 1
                        portsAvailable[recvIP] = portsSlavesClient

                    s[0] = value
                    s[1] = 0
            except:
                staticTimeStamp[recvIP] = [value,0]
                Alive[recvIP] = 1
                portsAvailable[recvIP] = portsSlavesClient

def recevHeartBeat(portsAvailable,rootIP, port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind ("tcp://%s:%s" % (rootIP, port))
    topic = "1"
    socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    timeStam = {}
	
    pCheckTime = threading.Thread(target=checkLive, args=(portsAvailable,timeStam)) 
    pCheckTime.start()

    while True:
        topic, recvIP = socket.recv_string().split()
        logging.info("alive from IP={}".format(recvIP))
        try:
            timeStam[recvIP] += 1
        except:
            timeStam[recvIP] = 1
        
if __name__ == "__main__": 
    pass
    # constants().N_DATABASE_SLAVES += 1
    # recevHeartBeat()


	
