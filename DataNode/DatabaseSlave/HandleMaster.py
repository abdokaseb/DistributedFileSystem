import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from Constants import SLAVE_DATABASE_HOST,SLAVE_DATABASE_USER,SLAVE_DATABASE_PASSWORD,SLAVE_DATABASE_DATABASE


def communicate(port,masterIP):
    print("Database Slave start to take new users from the master, IP:Port {}:{}".format(masterIP,port))
    mydb = mysql.connector.connect(
        host=SLAVE_DATABASE_HOST,
        user=SLAVE_DATABASE_USER,
        passwd=SLAVE_DATABASE_PASSWORD,
        database=SLAVE_DATABASE_DATABASE,
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://%s:%s" % (masterIP,port))
    print("Database Slave start connection stablished IP:Port {}:{}".format(masterIP,port))
    while True:
        message = socket.recv_string()
        try:
            print("Database Slave received new user from master with querey {}".format(message))
            dbcursour.execute(message)
            print(message)
            socket.send_string("True")
        except:
            print("ERROR: Database Slave received new user from master with querey {} but can't been added to databse")
            socket.send_string("False")

if __name__ == '__main__':
    communicate("8001","192.168.137.189")
