import sys
import zmq
import time
import multiprocessing as mp
import mysql.connector

pushContext = zmq.Context()
pushSocket = pushContext.socket(zmq.PUSH)
pushSocket.hwm = 10
# change this ip (client Device) with something general
pushSocket.bind("tcp://"+"*:"+"8002")

counter =0


for i in range (10000):
    print ("BEFORE SEND")
    pushSocket.send_string(str(counter))
    counter = counter+ 1


pushSocket.close()



