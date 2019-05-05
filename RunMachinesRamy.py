import sys
import os

try:
    typeRun = sys.argv[1]
    if typeRun == 'Client':
        os.system("set PYTHONUNBUFFERED=1&C:/Users/ramym/Anaconda3/python.exe Client/AccessFS.py")
    elif typeRun == 'MasterTracker':
        os.system("C:/Users/ramym/Anaconda3/python.exe MasterTracker/lookUpTableInit.py")
        os.system(
            "set PYTHONUNBUFFERED=1&C:/Users/ramym/Anaconda3/python.exe MasterTracker/MasterMachine.py > ./logs/MasterTracker.log")
    elif typeRun == 'DataNode':
        machineID = sys.argv[2]
        os.system(
            "C:/Users/ramym/Anaconda3/python.exe DataNode/DataNode.py {0} > ./logs/DataNode{0}.log".format(machineID))
    elif typeRun == 'DatabaseMaster':
        withDNS = 1
        if len(sys.argv) > 2:
            withDNS = sys.argv[2]
        os.system(
            "C:/Users/ramym/Anaconda3/python.exe MasterTracker/DatabaseMaster/TableInit.py")
        os.system("set PYTHONUNBUFFERED=1&C:/Users/ramym/Anaconda3/python.exe MasterTracker/DatabaseMaster/DatabaseMaster.py {} > ./logs/DatabaseMaster.log".format(withDNS))
    elif typeRun == 'DatabaseSlave':
        machineID = sys.argv[2]
        os.system("C:/Users/ramym/Anaconda3/python.exe DataNode/DatabaseSlave/TableInit.py")
        os.system(
            "set PYTHONUNBUFFERED=1&C:/Users/ramym/Anaconda3/python.exe DataNode/DatabaseSlave/DatabaseSlave.py {0} > ./logs/DatabaseSlave{0}.log".format(machineID))
    else:
        raise ValueError('error in arguments')
except:
    print("Error in arguments")
    print("Please write the arguments correctly here is some examples run:write:example")
    print("MasterTracker: the first argument should be MasterTracker")
    print("\t\tpython3 RunMachines.py MasterTracker\n")
    print("DatabaseMaster: the first argument should be DatabaseMaster")
    print("\t\tpython3 RunMachines.py DatabaseMaster\n")
    print("DataNode: the first argument should be DataNode, the second argument should be machine ID")
    print("\t\tpython3 RunMachines.py DataNode 2\n")
    print("DatabaseSlave: the first argument should be DatabaseSlave, the second argument should be machine ID")
    print("\t\tpython3 RunMachines.py DatabaseSlave 2\n")
    print("Client: the first argument should be Client")
    print("\t\tpython3 RunMachines.py Client\n")
