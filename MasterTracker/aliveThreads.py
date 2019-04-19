import sys
import zmq
import time
import threading 
import mysql.connector
import copy
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level="INFO",filename='logs/aliveMastertraker.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
from Constants import portsDatanodeClient


machineIP = getMyIP()

def checkLive(portsAvailable,timeStam,topics):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData",
        autocommit = True
    )

    dbcursour = mydb.cursor()
    dbcursour.execute("UPDATE machines SET isAlive = 0")
    # mydb.commit()
    dieSQL = "UPDATE machines SET isAlive = 0 WHERE id = %s "
    lifeSQL = "UPDATE machines SET isAlive = 1, IP = INET_ATON(%s) WHERE id = %s "

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
                if s[1] > 3 and Alive[int(key)] != 0:
                    Alive[int(key)] = 0
                    dbcursour.execute(dieSQL,(key,))
                    # mydb.commit()
                    logging.info("machine with ID={} and IP={} dead".format(key,recvIP))
                    print("machine with ID={} and IP={} dead".format(key,recvIP))
                    portsAvailable[recvIP] = []
                    
                    
            else:
                if Alive[int(key)] == 0:
                    logging.info("machine with ID={} and IP={} is alive".format(key,recvIP))
                    print("machine with ID={} and IP={} is alive".format(key,recvIP))
                    Alive[int(key)] = 1
                    dbcursour.execute(lifeSQL,(recvIP,key))
                    # mydb.commit()

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
        logging.info("alive from IP={}".format(recvIP))
        timeStam[topic][0] += 1
        timeStam[topic][1] = recvIP

if __name__ == "__main__": 
    recevHeartBeat()


	
