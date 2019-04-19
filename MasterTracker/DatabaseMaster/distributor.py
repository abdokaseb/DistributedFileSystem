import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
import os

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from HandleSlaves import SendSlave
from Constants import portsDatanodeClient
from Util import getLogger



def distributorF(qSQLs,ports):
    getLogger().info("Distrupeter up")
    slavesProcesses = mp.Pool(len(ports))
    manager = mp.Manager()
    qs = [manager.Queue() for _ in range(len(ports))]
    slavesProcesses.starmap_async(SendSlave,[(ports[i], qs[i]) for i in range(len(ports))])

    while True:
        message = qSQLs.get()
        getLogger().info("Distuputer send message {}".format(message))
        for q in qs: q.put(message)

        


if __name__ == '__main__':
    pass
    # portsMasterClient = ["5556"]

    # mydb = mysql.connector.connect(
    #     host="localhost",
    #     user="root",
    #     passwd="",
    #     database="lookUpData"
    # )

    # dbcursour = mydb.cursor()

    # dbcursour.execute("select IP from machines")
    # IPsRow = dbcursour.fetchall()
    # IPsM =  [str(IP[0]) for IP in IPsRow]

    # portsAvailable = mp.Manager().dict()
    # for IP in IPsM:
    #     portsAvailable[IP] = [port for port in portsDatanodeClient]
    
    # ps = [mp.Process(target = communicate,args=(portsAvailable,port)) for port in portsMasterClient]
    # for p in ps: p.start()
    # for p in ps: p.join()

