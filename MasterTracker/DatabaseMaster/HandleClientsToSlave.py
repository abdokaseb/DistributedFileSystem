import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import random

def communicate(portsAvailable,port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)
    while True:
        for slaveIP in portsAvailable.keys():
            m = socket.recv_string()
            print(m)

            portIndex = random.randint(0,len(portsAvailable[slaveIP])-1)
            socket.send_string('{}:{}'.format(slaveIP,portsAvailable[slaveIP][portIndex]))

        

    

if __name__ == '__main__':
    portsMasterClient = ["5556"]
    portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select IP from machines")
    IPsRow = dbcursour.fetchall()
    IPsM =  [str(IP[0]) for IP in IPsRow]

    portsAvailable = mp.Manager().dict()
    for IP in IPsM:
        portsAvailable[IP] = [port for port in portsDatanodeClient]
    
    ps = [mp.Process(target = communicate,args=(portsAvailable,port)) for port in portsMasterClient]
    for p in ps: p.start()
    for p in ps: p.join()

