import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
from Util import getLogger


def communicate(port,masterIP):
    getLogger().info("Database Slave start to take new users from the master, IP:Port {}:{}".format(masterIP,port))
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpDataSlave",
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://%s:%s" % (masterIP,port))
    getLogger().info("Database Slave start connection stablished IP:Port {}:{}".format(masterIP,port))
    while True:
        message = socket.recv_string()
        try:
            getLogger().info("Database Slave received new user from master with querey {}".format(message))
            dbcursour.execute(message)
            print(message)
            socket.send_string("True")
        except:
            getLogger().error("Database Slave received new user from master with querey {} but can't been added to databse")
            socket.send_string("False")

if __name__ == '__main__':
    communicate("8001","192.168.137.189")
