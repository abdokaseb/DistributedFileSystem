import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import random
import os
import logging
logging.basicConfig(level="INFO",filename='logs/MasDBDNSforDatabase.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from Constants import portsDatanodeClient
from Util import getMyIP

def communicate(portsAvailable,port):
    # logging.info("Port {} start to listen to clients".format(port))
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (getMyIP(),port))
    while True:
        for slaveIP in portsAvailable.keys():
            m = socket.recv_string()
            portIndex = random.randint(0,len(portsAvailable[slaveIP])-1)
            socket.send_string('{}:{}'.format(slaveIP,portsAvailable[slaveIP][portIndex]))
            # logging.info("Port {} replied with IP:Port number {}:{}".format(port,portsAvailable[slaveIP][portIndex]))

        

    

if __name__ == '__main__':
    portsMasterClient = ["5556"]

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData",
        autocommit = True
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select INET_NTOA(IP) from machines")
    IPsRow = dbcursour.fetchall()
    IPsM =  [str(IP[0]) for IP in IPsRow]

    portsAvailable = mp.Manager().dict()
    for IP in IPsM:
        portsAvailable[IP] = [port for port in portsDatanodeClient]
    
    ps = [mp.Process(target = communicate,args=(portsAvailable,port)) for port in portsMasterClient]
    for p in ps: p.start()
    for p in ps: p.join()

