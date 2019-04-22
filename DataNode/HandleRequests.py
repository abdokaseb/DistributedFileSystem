import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
import time
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import math
from Constants import USERACTIONS as ACTIONS,MASTER_FILESYSTEM_MACHINE_IP,masterPortUploadSucess
from Util import getMyIP,getLogger
from doneWork import uploadSucess

def communicate(port,DIR,machineIP,machineID):
    getLogger().info("IP:Port {}:{} to responed requests from the clients".format(machineIP,port))
    # print("inside process "+port)

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (machineIP,port))
    while True:
        #  Wait for next request from client
        message = socket.recv_string().split()
        # print(message)
        fileName = message[1] + '_' + message[2]
        action = ACTIONS[message[0]]
        getLogger().info("Port {} receive from client -> action is {}, file name is {}".format(port,action,fileName))
        # print(port)
        socket.send_string(
            "client operation info and user Id and have been reveived")
        # print("afterrrrrrrrrrrr seeeend")

        if action == ACTIONS['UPLOAD']:
            # receive client upload port
            ipPort = tuple(socket.recv_string().split(':'))
            #getLogger().info(" after receiving ports from clinet  " +
            #             " %s : %s " % ipPort)
            print("afterrrrrrrrrrrr seeeend")
            socket.send_string('pull push socket ip have been received')            
            uploadProcess = mp.Process(
                target=uploadFile, args=(ipPort, DIR, fileName,machineID))
            uploadProcess.start()
            #result = uploadFile(ipPort, fileName)
        elif action == ACTIONS['DOWNLOAD']:
            ipPort = socket.recv_string()
            #getLogger().info(" after receiving ports from clinet  "+ipPort)

            socket.send_string('pull push socket ip have been received')
            
            downloadProcess = mp.Process(target=downloadFile, args=(
                ipPort, DIR, fileName, message[3], message[4], message[5]))
            #result = downloadFile(ipPort, message[1], message[2], message[3], message[4], message[5])
            downloadProcess.start()


def uploadFile(ipPort, DIR, fileName,machineID):
    getLogger().info("IP:Port {} in client start uploading file name is {}".format(ipPort,fileName))
    context = zmq.Context()
    pullSocket = context.socket(zmq.PULL)
    pullSocket.hwm = 10
    pullSocket.connect("tcp://%s:%s" % ipPort)

    getLogger().info(" Successful connect to the pull socket for upload and IP:Port is %s:%s" % ipPort)
    print(
        " Successful connect to the pull socket and port is  "+"%s:%s" % ipPort)

    fileobj = open(DIR+fileName, 'wb+')

    #getLogger().info(" start receiving the file  in dir = " +
    #             DIR+"  and file name is = " + fileName)

    counter = 0
    while True:
        chunk = pullSocket.recv()
        print(counter)
        counter = counter+1
        if chunk is b'':
            print('condition satisfied')
            getLogger().info(" IP:Port is %s:%s" % ipPort + " Finished upload from client")
            break
        fileobj.write(chunk)

    fileobj.close()
    pullSocket.close()

    #getLogger().info(" after finished upload the file  ")

    ######################3
    ####### here we will norify the tracker
    userID,realFileName = fileName.split('_',1)
    uploadSucess(MASTER_FILESYSTEM_MACHINE_IP,masterPortUploadSucess,userID, machineID, realFileName)
    return 0


def downloadFile(ipPort, DIR, fileName, partNum, chunkSize, numberOfParts):
    getLogger().info("IP:Port {} in client start downloading file name is {}".format(ipPort,fileName))
    ## receive download port from the client and connect to it

    context = zmq.Context()
    pushSocket = context.socket(zmq.PUSH)
    print(ipPort)
    pushSocket.connect("tcp://"+ipPort)
    getLogger().info(" Successful connect to the pull socket for download and IP:Port is %s" % ipPort)

    #getLogger().info(" after connectiong  to the push socket  ")

    #getLogger().info(" file parameters     file in  " + DIR + "  " +
    #             fileName+"  part number ="+partNum+"  chunkSize = "+chunkSize + " totla number of parts ="+numberOfParts)

    with open(DIR+fileName, "rb") as f:
        f.seek(0, 2)  # move the cursor to the end of the file
        size = f.tell()
        size = int(size/int(numberOfParts))
        f.seek(int(partNum)*size, 0)
        if partNum != int(numberOfParts)-1:
            for i in range(math.floor(size/int(chunkSize))):
                chunk = f.read(int(chunkSize))
                pushSocket.send(chunk)

            rest = size-math.floor(size/int(chunkSize))*int(chunkSize)
            if rest > 0:
                chunk = f.read(rest)
                pushSocket.send(chunk)
        else:
            chunk = f.read(int(chunkSize))
            while chunk:
                pushSocket.send(chunk)
                chunk = f.read(int(chunkSize))

    f.close()
    pushSocket.send(b'')
    print("finish Send")
    time.sleep(3)
    #getLogger().info("IP:Port is %s" % ipPort + " Finished download to client")

    #getLogger().info(" end download with partnumber = "+partNum)

    ######################3
    ####### here we will norify the tracker

    return 0
