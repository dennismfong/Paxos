##CS171 FINAL PROJECT
##TIMOTHY KWONG
##DENNIS FONG
##
##CLI IMPLEMENTATION

import socket
import sys

MYID = '127.0.0.1'

ports = [None] * 4
for i in range(4):
    ports[i] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ##ports[0] = map1
    ##ports[1] = map2
    ##ports[0] = reducer
    ##ports[1] = prm/replicator
port_nums = [5001, 5002, 5003, 5004]
port_nums[3] = int(sys.argv[1])

def setup():
#    while True:
#        try:
#            ports[0].connect((MYID, 5001)) 
#            break
#        except Exception:
#            pass
#    while True:
#        try:
#            ports[1].connect((MYID, 5002))
#            break
#        except Exception:
#            pass
#    while True:
#        try:
#            ports[2].connect((MYID, 5003))
#            break
#        except Exception:
#           pass
    while True:
        try:
            ports[3].connect((MYID, port_nums[3]))
            break
        except Exception:
            pass

def cli_main():
    while True:
        command = raw_input("Enter command: \n")
        if(len(command) == 0):
            continue
        arg = command.split()
        message = arg[0]
        if (arg[0] == 'map'):
            #process_map(arg[1])
            print "Mapping " + arg[1]
        elif (arg[0] == 'reduce'):
            for i in range(1, len(arg)):
                if(i < len(arg) - 1):
                    message = message + arg[i] + ','
                else:
                    message = message + arg[i] + " "
            print "Reducing!"
            #process_reduce(message)
        elif (arg[0] == 'replicate'):
            message = message + ',' + arg[1] + " "
            print "Sent replicate"
        elif (arg[0] == 'stop'):
            print "Stopping PRM"
            pass
        elif (arg[0] == 'resume'):
            print "Resuming PRM"
            pass
        elif (arg[0] == 'total'):
            for i in range(1, len(arg)):
                if(i < len(arg) - 1):
                    message = message + arg[i] + ','
                else:
                    message = message + arg[i] + " "   
            print "Totaling!"
        elif (arg[0] == 'print'):
            pass
        elif (arg[0] == 'merge'):
            message = message + ',' + arg[1] + ',' + arg[2] + " "
            print "Merging " + arg[1] + ' and ' + arg[2]
        elif (arg[0] == 'exit'):
            print "Exiting!"
            return
        else:
            print 'Invalid argument!'
        while True:
            try:
                ports[3].sendall(message + " ")
                break
            except Exception:
                pass

setup()
print "Done with setup"
cli_main()

##########################
##
##def replicate(filename):
#### placeholders for code referencing ##
##    PRM_logs = {} 
##    PRM_logs[log_number] = {}       ##log_number = whichever log the file is stored in order
##    PRM_logs[log_number]['words'] = {}
##    PRM_logs[log_number]['name'] = filename
#### end of placeholders ##
##    for line in filename:
##        word = line.split()[0]      #word
##        wc = int(line.split()[1])   #word count
##        if word in PRM_logs[filename]['words']:     #word already exists in logged dict
##            PRM_logs[log_number]['words'][word] = PRM_logs[filename]['words'][word] + wc 
##        else:                                       #word doens't exist, add it
##            PRM_logs[log_number]['words'][word] = wc
##    rep_log = PRM_logs[log_number]
##    #send rep_log to other PRMs to replicate
##    
##            
############################
##
##def total(pos1, pos2):
##    total_count = 0
##    fpos1 = open(pos1, 'r')
##    fpos2 = open(pos2, 'r')
##    for line in fpos1:
##        word = line.split()
##        total_count = total_count + int(word[1])
##    for line in fpos2:
##        word = line.split()
##        total_count = total_count + int(word[1])
##    return total_count
##
##def print_log():
##    for filename in logs:
##        print filename
##
##def merge(pos1, pos2):
##    fpos1 = open(pos1, 'r')         #LINE FORMAT: [word] [count]
##    fpos2 = open(pos2, 'r')
##    output = {}
##    for line in fpos1:
##        word = line.split()
##        if word[0] == 'filename':
##            pass
##        elif word[0] not in output:   #NEW INSTANCE OF WORD
##            output[word[0]] = int(word[1])
##        else:                       #WORD ALREADY EXISTS
##            output[word[0]] = output[word[0]] + int(word[1])
##    for line in fpos2:
##        word = line.split()
##        if word[0] == 'filename':
##            pass
##        if word[0] not in output:   #NEW INSTANCE OF WORD
##            output[word[0]] = int(word[1])
##        else:                       #WORD ALREADY EXISTS
##            output[word[0]] = output[word[0]] + int(word[1])
##    for index in output:
##        print index + ': ' + str(output[index])


