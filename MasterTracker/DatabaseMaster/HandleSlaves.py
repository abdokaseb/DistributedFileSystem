import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json


def SendSlave(port,qSQLs):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.bind("tcp://*:%s" % port)
    
    getFromQueue = 1
    while True:
        if getFromQueue == 1:
            message = qSQLs.get()
        print("send to slaves port "+ port)
        socket.send_string(message)
        response = socket.recv_string()
        if response != "True":
            getFromQueue = 0
        else:
            getFromQueue = 1

            

if __name__ == '__main__':
    pass
    # portsMasterClient = ["5556"]
    # portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]

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

