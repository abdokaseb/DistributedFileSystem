import sys
import zmq
import time
import multiprocessing as mp
import mysql.connector

context = zmq.Context()
pullSocket = context.socket(zmq.PULL)
pullSocket.hwm = 10
pullSocket.connect("tcp://localhost:"+"8002")


while True :
    result=pullSocket.recv_string()
    print (result)
    time.sleep(3)
    pullSocket.connect("tcp://localhost:"+"8002")





