import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json



def communicate(port,masterIP):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="testSlave",
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://%s:%s" % (masterIP,port))

    while True:
        message = socket.recv_string()
        try:
            dbcursour.execute(message)
            print(message)
            socket.send_string("True")
        except:
            socket.send_string("False")

if __name__ == '__main__':
    communicate("8001","192.168.137.189")
