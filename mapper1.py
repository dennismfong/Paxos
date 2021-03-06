#MAPPER 1 (ports[0])

import socket

file_tag = '_I_1.txt'
LOCALHOST = '127.0.0.1'
#PORT = int(sys.argv[1])
PORT = 5001
BUFFERSIZE = 12288

servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
servsock.bind((LOCALHOST, PORT))
servsock.listen(1)
stream, addr = servsock.accept()

def mapper1():
    global stream, BUFFERSIZE
    while True:
        try:
            print "Waiting for file to map"
            data = stream.recv(BUFFERSIZE)
            print data
            streamData = data.split()
            for mapData in streamData:
                mapArgs = mapData.split(',')
                f = open(mapArgs[1], 'r') #FILENAME
                f2 = open(mapArgs[1][:-4] + file_tag, 'w+') #NEW FILE
                f.seek(int(mapArgs[2]))
                temp = f.read(int(mapArgs[3]) - int(mapArgs[2]))
                words = temp.split()
                w_dict = {}
                for word in words:
                    if word not in w_dict:
                        w_dict[word] = 1
                    else:
                        w_dict[word] += 1
                for word in w_dict:
                    f2.write(word + ' ' + str(w_dict[word]) + '\n')
                f2.close()
                f.close()

        except KeyboardInterrupt:
            break

mapper1()

    
