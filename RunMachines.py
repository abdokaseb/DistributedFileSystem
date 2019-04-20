import sys 
import os

try:
    typeRun = sys.argv[1]
    if typeRun == 'Client':
        os.system("python3 Client/AccessFS.py")
    elif typeRun == 'MasterTracker':
        os.system("python3 MasterTracker/lookUpTableInit.py")
        os.system("python3 MasterTracker/MasterMachine.py")
    elif typeRun == 'DataNode': 
        machineID = sys.argv[2]
        os.system("python3 DataNode/DataNode.py "+machineID)
    elif typeRun == 'DatabaseMaster':
        os.system("python3 MasterTracker/DatabaseMaster/TableInit.py")
        os.system("python3 MasterTracker/DatabaseMaster/DatabaseMaster.py")
    elif typeRun == 'DatabaseSlave':
        machineID = sys.argv[2]
        os.system("python3 DataNode/DatabaseSlave/TableInit.py")
        os.system("python3 DataNode/DatabaseSlave/DatabaseSlave.py "+machineID)
    else:
        raise ValueError('error in arguments')
except:
    print("Error in arguments")
    print("Please write the arguments correctly here is some examples run:write:example")
    print("MasterTracker: the first argument should be MasterTracker")
    print("\t\tpython3 RunMachines MasterTracker\n")
    print("DatabaseMaster: the first argument should be DatabaseMaster")
    print("\t\tpython3 RunMachines DatabaseMaster\n")
    print("DataNode: the first argument should be DataNode, the second argument should be machine ID")
    print("\t\tpython3 RunMachines DataNode 2\n")
    print("DatabaseSlave: the first argument should be DatabaseSlave, the second argument should be machine ID")
    print("\t\tpython3 RunMachines DatabaseSlave 2\n")
    print("Client: the first argument should be Client")
    print("\t\tpython3 RunMachines Client\n")


    