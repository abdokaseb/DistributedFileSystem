import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector



def communicate(port,IP):
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
    socket.bind("tcp://%s:%s" % (IP,port))

    while True:
        message = socket.recv_string()
        try:
            print(message)
            dbcursour.execute(message)
            result= dbcursour.fetchall()
            print(result[0][0])
            socket.send_string("{}".format(result[0][0]))
        except:
            socket.send_string("-2")

if __name__ == '__main__':
    portsDatabaseClient = ["5756","5757","5758"]
    ps = [mp.Process(target=communicate,args=(port,"127.0.0.1")) for port in portsDatabaseClient]
    for p in ps: p.start()
    # communicate("5556","172.28.182.67")
