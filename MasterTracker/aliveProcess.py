import sys
import zmq
import time
import multiprocessing 
import mysql.connector
import copy

def checkLive(timeStam,topics,mydb):
    dbcursour = mydb.cursor()
    staticTimeStamp = {}
    Alive = [0]*(len(topics)+1)
    for topic in topics:
        staticTimeStamp[topic] = [timeStam[topic],0]

    dbcursour.execute("UPDATE machines SET isAlive = 0")
    mydb.commit()
    SQL = "UPDATE machines SET isAlive = %s WHERE id = %s "
    
    while True:
        time.sleep(1)
        for key, s in staticTimeStamp.items():
            value = timeStam[key]
            if (s[0] >= value):
                s[1] += 1
                if s[1] > 3:
                    Alive[int(key)] = 0
                    dbcursour.execute(SQL,("0",key))
                    mydb.commit()
                    
            else:
                if Alive[int(key)] == 0:
                    Alive[int(key)] = 1
                    dbcursour.execute(SQL,("1",key))
                    mydb.commit()

                s[0] = value
                s[1] = 0

def recevHeartBeat(port = "5556"):    
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
    # topics = ["1"]

    # Socket to talk to server
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind ("tcp://*:%s" % port)
    [socket.setsockopt_string(zmq.SUBSCRIBE, topic) for topic in topics]


    timeStam = multiprocessing.Manager().dict() ##changed line
    for topic in topics:
        timeStam[topic] = 0
	
    pCheckTime = multiprocessing.Process(target=checkLive, args=(timeStam,topics,mydb)) ##changed line
    pCheckTime.start()

    while True:
        topic, _ = socket.recv().split()
        topic = str(topic)[2:-1]
        timeStam[topic] += 1

if __name__ == "__main__": 
    recevHeartBeat()


	