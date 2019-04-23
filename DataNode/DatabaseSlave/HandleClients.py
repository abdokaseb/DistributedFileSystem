import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
from Util import getLogger
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from Constants import SLAVE_DATABASE_HOST,SLAVE_DATABASE_USER,SLAVE_DATABASE_PASSWORD,SLAVE_DATABASE_DATABASE


def communicate(port,IP):
    getLogger().info("Database slave start to listen for clients in IP:Port {}:{}".format(IP,port))
    mydb =  mysql.connector.connect(
        host=SLAVE_DATABASE_HOST,
        user=SLAVE_DATABASE_USER,
        passwd=SLAVE_DATABASE_PASSWORD,
        database=SLAVE_DATABASE_DATABASE,
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (IP,port))
    getLogger().info("Database slave start to listen for clients in IP:Port {}:{} connection stablished correctly".format(IP,port))
    
    while True:
        message = socket.recv_string()
        try:
            getLogger().info("Port {} received message {}".format(port,message))
            dbcursour.execute(message)
            result= dbcursour.fetchall()
            getLogger().info("Port {} received message {} replied with id={}".format(port,message,result[0][0]))
            socket.send_string("{}".format(result[0][0]))
        except:
            getLogger().error("Port {} received message {} but database error happen".format(port,message))
            socket.send_string("-2")

if __name__ == '__main__':
    portsDatabaseClient = ["5756","5757","5758"]
    ps = [mp.Process(target=communicate,args=(port,"127.0.0.1")) for port in portsDatabaseClient]
    for p in ps: p.start()
    # communicate("5556","172.28.182.67")
