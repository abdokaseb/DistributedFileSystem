import sys
import zmq
import time
import multiprocessing as mp
import mysql.connector

from HandleRequests import communicate


portsDatanode = ["6001", "6002", "6003", "6004", "6005", "6006"]




if __name__ == "__main__":

    with mp.Pool(len(portsDatanode)) as dataProcesses:
        dataProcesses.map(communicate,portsDatanode)
        dataProcesses.join()
