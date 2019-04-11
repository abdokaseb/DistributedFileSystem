import sys
import zmq
import time
import threading 
import mysql.connector
import copy
portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]

def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())
machineIP = getMachineIP()

def checkLive(portsAvailable,timeStam,topics):
    staticTimeStamp = {}
    Alive = [0]*(len(topics)+1)
    for topic in topics:
        staticTimeStamp[topic] = [timeStam[topic][0],0]

    while True:
        time.sleep(1)
        for key, s in staticTimeStamp.items():
            value,recvIP = timeStam[key]
            if (s[0] >= value):
                s[1] += 1
                if s[1] > 3 and recvIP != '0':
                    print("machine with ID={} and IP={} dead".format(key,recvIP))
                    portsAvailable[recvIP] = []
                    
                    
            else:
                if Alive[int(key)] == 0:
                    print("machine with ID={} and IP={} is alive".format(key,recvIP))
                    Alive[int(key)] = 1
                    portsAvailable[recvIP] = portsDatanodeClient

                s[0] = value
                s[1] = 0

def recevHeartBeat(portsAvailable,rootIP, port,IDs):
    # Socket to talk to server
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind ("tcp://%s:%s" % (rootIP, port))
    [socket.setsockopt_string(zmq.SUBSCRIBE, topic) for topic in IDs]

    timeStam = {}
    for topic in IDs:
        timeStam[topic] = [0,"0"]
	
    pCheckTime = threading.Thread(target=checkLive, args=(portsAvailable,timeStam,IDs)) 
    pCheckTime.start()

    while True:
        topic, recvIP = socket.recv_string().split()
        timeStam[topic][0] += 1
        timeStam[topic][1] = recvIP

if __name__ == "__main__": 
    recevHeartBeat()


	