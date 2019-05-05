import zmq
import time
import sys
import multiprocessing as mp
import json
import os

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from Constants import portsDatanodeClient
from Util import getMyIP

def SendSlave(port,qSQLs):
    print("Port {} start to sending new users to slaves".format(port))
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.bind("tcp://%s:%s" % (getMyIP(),port))
    
    getFromQueue = 1
    while True:
        if getFromQueue == 1:
            message = qSQLs.get()
        # print("send to slaves port "+ port)
        socket.send_string(message)
        print("Port {} send {} slave".format(port,message))
        response = socket.recv_string()
        if response == "True":
            getFromQueue = 1
        else:
            getFromQueue = 0

            

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

