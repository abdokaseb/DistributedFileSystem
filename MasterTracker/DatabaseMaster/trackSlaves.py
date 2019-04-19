import sys
import zmq
import time
import threading 
import mysql.connector
import copy
portsSlavesClient = ["5756","5757","5758"]
# portsSlavesClient = ["10001","10002","10003","10004","10005","10006"]
N_DATABASE_SLAVES = 5

def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())
machineIP = getMachineIP()

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
                        print("Database Slaves with IP={} dead".format(recvIP))
                        portsAvailable[recvIP] = []
                        
                        
                else:
                    if Alive[recvIP] == 0:
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
        try:
            timeStam[recvIP] += 1
        except:
            timeStam[recvIP] = 1
        
if __name__ == "__main__": 
    pass
    # constants().N_DATABASE_SLAVES += 1
    # recevHeartBeat()


	