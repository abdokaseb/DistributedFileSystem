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

def checkLive(portsAvailable,timeStam,topics,mydb):
    dbcursour = mydb.cursor()
    staticTimeStamp = {}
    Alive = [0]*(len(topics)+1)
    for topic in topics:
        staticTimeStamp[topic] = [timeStam[topic][0],0]

    dbcursour.execute("UPDATE machines SET isAlive = 0")
    mydb.commit()
    dieSQL = "UPDATE machines SET isAlive = 0 WHERE id = %s "
    lifeSQL = "UPDATE machines SET isAlive = 1, IP=%s WHERE id = %s "

    while True:
        time.sleep(1)
        for key, s in staticTimeStamp.items():
            value,recvIP = timeStam[key]
            if (s[0] >= value):
                s[1] += 1
                if s[1] > 3:
                    Alive[int(key)] = 0
                    dbcursour.execute(dieSQL,(key,))
                    mydb.commit()

                    portsAvailable[recvIP] = []
                    
                    
            else:
                if Alive[int(key)] == 0:
                    Alive[int(key)] = 1
                    dbcursour.execute(lifeSQL,(recvIP,key))
                    mydb.commit()
                    
                    portsAvailable[recvIP] = portsDatanodeClient

                s[0] = value
                s[1] = 0

def recevHeartBeat(portsAvailable,rootIP, port):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select id from machines")
    ids = dbcursour.fetchall()
    topics =  [str(id[0]) for id in ids]

    # Socket to talk to server
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind ("tcp://%s:%s" % (rootIP, port))
    [socket.setsockopt_string(zmq.SUBSCRIBE, topic) for topic in topics]

    timeStam = {}
    for topic in topics:
        timeStam[topic] = [0,"0"]
	
    pCheckTime = threading.Thread(target=checkLive, args=(portsAvailable,timeStam,topics,mydb)) 
    pCheckTime.start()

    while True:
        topic, recvIP = socket.recv_string().split()
        timeStam[topic][0] += 1
        timeStam[topic][1] = recvIP

if __name__ == "__main__": 
    recevHeartBeat()


	