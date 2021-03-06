#REDUCER

import socket

file_tag = '_reduced.txt'
LOCALHOST = '127.0.0.1'
##PORT = int(sys.argv[1])
PORT = 5003
BUFFERSIZE = 12288

servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
servsock.bind((LOCALHOST, PORT))
servsock.listen(1)
stream, addr = servsock.accept()

def reducer():
    global BUFFERSIZE
    while True:
        try:
            print "Waiting for stuff to reduce"
            data = stream.recv(BUFFERSIZE)
            streamData = data.split()
#            streamData = ['file1_I_2.txt,file2_I_2.txt']
            
            for redData in streamData:
                redArgs = redData.split(',')
                f_new = open(redArgs[1][:-8] + file_tag, 'w') #NEW FILE
                w_dict = {}
                for doc in redArgs:
                    print doc
                    if doc == "reduce":
                        continue
                    f = open(doc, 'r')
                    for line in f:
                        f_split = line.split()
                        if f_split[0] not in w_dict:
                            w_dict[f_split[0]] = int(f_split[1])
                        else:
                            w_dict[f_split[0]] += int(f_split[1])
                    f.close()
                for word in w_dict:
                    f_new.write(word + ' ' + str(w_dict[word]) + '\n')
                f_new.close()

        except KeyboardInterrupt:
            break;
##
##finally:
##    servsock.close()
reducer()

