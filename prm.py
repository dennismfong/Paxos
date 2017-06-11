#!/usr/bin/env python2
# Dennis Fong, Tim Kwong
# 8234338, 8334492
# 22 May 2017

# ./prmTest siteID fileOfIPs port

import sys
import socket
import time
import Queue
import threading

q = Queue.Queue()
MYIP = '127.0.0.1'
LOCALHOST = '127.0.0.1'
MYID = int(sys.argv[1])
PORT = int(sys.argv[3])

# int, int
# ballotNumber, aSiteID
LOCALCOUNT = 0
BALLOTNUM = [0, MYID]
ACCEPTNUM = [0, 0]

# string (file name to be replicated)
ACCEPTVAL = "null"

# string (file name proposed to be replicated)
PROPOSEDVAL = None

# string to string
# siteID to IP
IPDICT = {}

# for testing
# string to string
# siteID to port
PORTDICT = {}

# the nested 3d dictionary
THELOG = {} 

#the current index of the log we are working on
log_number = 0

# string to socket
# ID to socket
SOCKDICT = {}

NUMACKS = {}
NUMACCEPTS = {}
LEADERS = {}
ISRUNNING = 1

# update the LISTOFIPS dict from config file and MYIP
def setupConfig():
    with open(sys.argv[2], 'r') as configFile:
        for line in configFile:
            line = line.split()
            if str(MYID) not in line[0]:
                PORTDICT[line[0]] = line[1]

# connect to all other PRMs
def setupPorts():
    for siteID in PORTDICT:
        addr = (LOCALHOST, int(PORTDICT[siteID]))
        SOCKDICT[siteID] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        SOCKDICT[siteID].connect(addr)

#  messages sent with spaces after each other, ballots separated by commas
def checkStream():
    global BALLOTNUM, PROPOSEDVAL, NUMACKS, NUMACCEPTS, ISLEADER, ISRUNNING, ACCEPTNUM, ACCEPTVAL, LOCALCOUNT
    try:
        rawData = q.get()
        print rawData
        splitData = rawData.split()
        for ballot in splitData:
            ballotArgs = ballot.split(',')
            if "stop" in ballot:
                ISRUNNING = 0
            if "resume" in ballot:
                ISRUNNING = 1
            if (ISRUNNING):
                if "replicate" in ballot:
                    # replicate,fileName
                    LOCALCOUNT += 1
                    BALLOTNUM[0] = LOCALCOUNT
                    PROPOSEDVAL = ballotArgs[1]
                    balKey = str(BALLOTNUM[0]) + "." + str(BALLOTNUM[1])
                    NUMACKS[balKey] = 0
                    LEADERS[balKey] = 1
                    sendPrepare()
                if "prepare" in ballot:
                    # prepare,ballot.num, ballot.ID
                    incomingNum = int(ballotArgs[1])
                    incomingID = int(ballotArgs[2])
                    incomingBallot = [incomingNum, incomingID]
                    if firstGreater(incomingBallot, BALLOTNUM):
                        BALLOTNUM[0] = incomingNum
                        BALLOTNUM[1] = incomingID
                        sendAck(incomingBallot)
                if "ack" in ballot:
                    # ack,proposedBal.num,proposedBal.ID,acceptBal.num,acceptBal.ID,acceptVal
                    print "Got ack"
                    ackKey = ballotArgs[1] + "." + ballotArgs[2]
                    NUMACKS[ackKey] += 1
                    if NUMACKS[ackKey] == 1:
                        incomingBal  = [int(ballotArgs[1]), int(ballotArgs[2])]
                        incomingAcceptBal = [int(ballotArgs[3]), int(ballotArgs[4])]
                        incomingVal = ballotArgs[5]
                        if (incomingVal and ACCEPTVAL) is "null":
                            myTempVal = PROPOSEDVAL
                        else:
                            #does this work? only 3 nodes so maybe?
                            if firstGreater(ACCEPTNUM, incomingAcceptBal):
                                myTempVal = ACCEPTVAL
                            else:
                                myTempVal = incomingVal
                        leaderAccept(myTempVal, ackKey)
                if "accept" in ballot:
                    # accept,ballotNum.num,ballotNum.ID,myTempVal
                    acceptKey = ballotArgs[1] + "." + ballotArgs[2]
                    incomingBal = [int(ballotArgs[1]), int(ballotArgs[2])]
                    incomingAcceptVal = ballotArgs[3]
                    if acceptKey not in NUMACCEPTS:
                        NUMACCEPTS[acceptKey] = 1 
                        if firstGreater(incomingBal, BALLOTNUM):
                            ACCEPTNUM[0] = incomingBal[0]
                            ACCEPTNUM[1] = incomingBal[1]
                            ACCEPTVAL = incomingAcceptVal
                            cohortAccept(incomingBal, incomingAcceptVal)
                    elif NUMACCEPTS[acceptKey] == 1:
                        NUMACCEPTS[acceptKey] += 1
                        if firstGreater(incomingBal, BALLOTNUM):
                            ACCEPTNUM[0] = incomingBal[0]
                            ACCEPTNUM[1] = incomingBal[1]
                            ACCEPTVAL = incomingAcceptVal
                            if acceptKey in LEADERS:
                                decide()
                if "decide" in ballot:
                    # decide,filename/word+wc/word+wc/...
                    print ballotArgs[1]
                    stringToLog(ballotArgs[1])
                    # replicate from other node
                if "total" in ballot:
                    total(ballot)
                if "print" in ballot:
                    print_log()
                if "merge" in ballot:
                    merge(ballotArgs[1], ballotArgs[2])
    except Exception,e:
        print str(e)
            
def firstGreater(ballot1, ballot2):
    if ballot1[0] > ballot2[0]:
        return True
    elif ballot1[0] == ballot2[0]:
        if ballot1[1] > ballot2[1]:
            return True
        elif ballot1[1] == ballot2[1]:
            return True
        else:
            return False
    else:
        return False

def sendPrepare():
    for sock in SOCKDICT:
        SOCKDICT[sock].sendall("prepare," + str(BALLOTNUM[0]) + "," + str(BALLOTNUM[1]) + " ")
    print "Sent prepare"

def sendAck(ballot):
    # ballotNum, ballotID
    # int, int
    destination = str(ballot[1])
    message = "ack," + str(ballot[0]) + "," + str(ballot[1]) + "," + str(ACCEPTNUM[0]) + "," + str(ACCEPTNUM[1]) + "," + ACCEPTVAL
    
    SOCKDICT[destination].sendall(message)

def leaderAccept(tempAcceptVal, ackKey):
    for sock in SOCKDICT:
        SOCKDICT[sock].sendall("accept," + str(BALLOTNUM[0]) + "," + str(BALLOTNUM[1]) + "," + tempAcceptVal)
    NUMACCEPTS[ackKey] = 1
    
                               
def cohortAccept(b, v):
    for sock in SOCKDICT:
        SOCKDICT[sock].sendall("accept," + str(b[0]) + "," + str(b[1]) + "," + v)

def decide():
    print "Received majority, decide()"
    replicate(ACCEPTVAL)
                               
# PRM FUNCTIONS #
def merge(pos1, pos2):
    p1_dict = THELOG[pos1]['words']          #LINE FORMAT: [word] [count]
    p2_dict = THELOG[pos2]['words']
    output = {}
    for word in p1_dict:
        if word not in output:   #NEW INSTANCE OF WORD
            output[word] = p1_dict[word]
        else:                       #WORD ALREADY EXISTS
            output[word] = output[word] + p1_dict[word]
    for word in p2_dict:
        if word not in output:   #NEW INSTANCE OF WORD
            output[word] = p2_dict[word]
        else:                       #WORD ALREADY EXISTS
            output[word] = output[word] + p2_dict[word]
    for index in output:
        print index + ': ' + str(output[index])

def total(args):
    arglist = args.split(",")
    total_count = 0
    itera = 0
    pos_dicts = {}
    for pos in arglist:
        if itera == 0:
            itera = itera + 1
        else:
            pos_dicts[pos] = THELOG[pos]['words']
    for posdict in pos_dicts:
            for word in pos_dicts[posdict]:
                total_count = total_count + pos_dicts[posdict][word]
    return total_count

def print_log():
    for index in THELOG:
        print THELOG[index]['name']


def replicate(filename):
## placeholders for code referencing ##
    global log_number
    THELOG[log_number] = {}       ##log_number = whichever log the file is stored in order
    THELOG[log_number]['words'] = {}
    THELOG[log_number]['name'] = filename
## end of placeholders ##

    readfile = open(filename, 'r')
    for line in readfile:
        if (len(line.split()) != 2):
            pass
        else:
            word = line.split()[0]      #word
            wc = int(line.split()[1])   #word count
            if word not in THELOG[log_number]['words']:     # word doesn't exist
                THELOG[log_number]['words'][word] = wc 
            else:                                       # word does exist, increment it
                THELOG[log_number]['words'][word] += wc
    words = THELOG[log_number]['words']        
    rep_log = THELOG[log_number]['name'] + '/'
    for word in words:
        rep_log = rep_log + word + '+' + str(words[word]) + '/' 
    rep_log = rep_log.strip('/')
    for sock in SOCKDICT:
        SOCKDICT[sock].sendall("decide," + rep_log)    
    #send rep_log to other PRMs to replicate

    log_number = log_number + 1
    print "Finish local replicate"
    reset()
 
def stringToLog(logString):
    words = logString.split('/')
    updatedName = False          #check if log has updated name
    if log_number not in THELOG:
        THELOG[log_number] = {}
    THELOG[log_number]['words'] = {}
    for line in words:
        if not updatedName:
            THELOG[log_number]['name'] = line
            updatedName = True
        else:
            word = line.split('+')[0]
            wc = int(line.split('+')[1])
            THELOG[log_number]['words'][word] = wc                      
    reset()
    print "Paxos log done"

def reset():
    global BALLOTNUM, ACCEPTNUM, ACCEPTVAL
    BALLOTNUM = [0, MYID]
    ACCEPTNUM = [0, 0]
    ACCEPTVAL = "null"

def thread1():
    stream1, addr1 = servsock.accept()
    while True:
        try:
            data = stream1.recv(1024)
            q.put(data)
        except KeyboardInterrupt:
            servsock.close()

def thread2():
    stream2, addr2 = servsock.accept()
    while True:
        try: 
            data = stream2.recv(1024)            
            q.put(data)
        except KeyboardInterrupt:
            servsock.close()

def thread3():
    stream3, addr3 = servsock.accept()
    while True:
        try:
            data = stream3.recv(1024)
            q.put(data)
        except KeyboardInterrupt:
            servsock.close()

# the main function
setupConfig()
print "Config set up"

servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
servsock.bind((LOCALHOST, PORT))
servsock.listen(10)
time.sleep(3)

setupPorts()

t1 = threading.Thread(target = thread1)
t2 = threading.Thread(target = thread2)
t3 = threading.Thread(target = thread3)

t1.daemon = True
t2.daemon = True
t3.daemon = True

t1.start()
t2.start()
t3.start()

print "Ports setup"

# keep while loop running to checkStream()
try:
    while True:
        checkStream()
except KeyboardInterrupt:
    sys.exit()
finally:
    servsock.close()
    for sock in SOCKDICT:
        SOCKDICT[sock].close()
