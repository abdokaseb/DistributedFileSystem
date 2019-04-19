import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
import time


import math

ACTIONS = {'UPLOAD': 0, 'DOWNLOAD': 1}

DIR = "C:\\Users\\ramym\\Desktop\\nn\\"
#DIR = ''

def communicate(port):
    
    print ("inside process "+port)

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)
    while True:
        #  Wait for next request from client
        message = socket.recv_string().split()
        print(message)
        socket.send_string("client operation info and user Id and have been reveived")

        if ACTIONS[message[0]] == ACTIONS['UPLOAD']:
            ipPort = tuple(socket.recv_string().split(':'))  # receive client upload port
            socket.send_string('pull push socket ip have been received')
            fileName = message[1] +'_'+ message[2]  
            uploadProcess = mp.Process(target=uploadFile, args=(ipPort, fileName))
            uploadProcess.start()
            #result = uploadFile(ipPort, fileName)
        elif ACTIONS[message[0]] == ACTIONS['DOWNLOAD']:
            ipPort = socket.recv_string()
            socket.send_string('pull push socket ip have been received')
            fileName = message[1] + '_' + message[2]
            downloadProcess = mp.Process(target=downloadFile, args=(
                ipPort, fileName, message[3], message[4], message[5]))
            #result = downloadFile(ipPort, message[1], message[2], message[3], message[4], message[5])
            downloadProcess.start()
            print(result)


def uploadFile(ipPort,fileName):
    
    context = zmq.Context()
    pullSocket = context.socket(zmq.PULL)
    pullSocket.hwm = 10
    pullSocket.connect("tcp://%s:%s"%ipPort)

    fileobj = open(DIR+fileName, 'wb+')
    
    counter =0
    while True :
        chunk=pullSocket.recv()
        #print(counter)
        counter=counter+1
        if chunk is b'':
            print('condition satisfied')
            break
        fileobj.write(chunk)

    fileobj.close()    
    pullSocket.close() 

    ######################3 
    ####### here we will norify the tracker

    return 0


def downloadFile(ipPort,fileName, partNum, chunkSize, numberOfParts):

    ## receive download port from the client and connect to it
    
    context = zmq.Context()
    pushSocket = context.socket(zmq.PUSH)
    pushSocket.bind("tcp://"+ipPort)


    with open(DIR+fileName, "rb") as f:
        f.seek(0, 2)  # move the cursor to the end of the file
        size = f.tell()
        size = int(size/int(numberOfParts))
        f.seek(int(partNum)*size, 0)  
        if partNum != int(numberOfParts)-1:
            for i in range(math.floor(size/int(chunkSize))):
                chunk = f.read(int(chunkSize))
                pushSocket.send(chunk)
                print('data sended from '+partNum)

            rest = size-math.floor(size/int(chunkSize))*int(chunkSize)
            if rest>0:
                chunk = f.read(rest)
                pushSocket.send(chunk)
        else:
            chunk = f.read(int(chunkSize))
            while chunk:
                pushSocket.send(chunk)
                print('data sended from '+partNum)
                chunk = f.read(int(chunkSize))


    f.close()

    pushSocket.send(b'')

    
    ######################3
    ####### here we will norify the tracker

    return 0


