import zmq
import time
import sys
import multiprocessing as mp
import random
import os

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from Constants import portsDatanodeClient
from Util import getMyIP, getLogger

def communicate(portsAvailable,machineIP,port):
    getLogger().info("DNS with IP:Port {}:{} listen to clients".format(machineIP,port))
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (machineIP,port))
    while True:
        for slaveIP in portsAvailable.keys():
            m = socket.recv_string()
            portIndex = random.randint(0,len(portsAvailable[slaveIP])-1)
            socket.send_string('{}:{}'.format(slaveIP,portsAvailable[slaveIP][portIndex]))
            getLogger().info("DNS with port {} sent answer {}:{} to client".format(port,slaveIP,portsAvailable[slaveIP][portIndex]))

        

    

if __name__ == '__main__':
    portsMasterClient = ["5556"]

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpDataMaster",
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

