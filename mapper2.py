#MAPPER 2 (ports[1])

import socket

file_tag = '_I_2.txt'
LOCALHOST = '127.0.0.1'
##PORT = int(sys.argv[1])
##
##servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
##servsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
##servsock.bind((LOCALHOST, PORT))
##servsock.listen(1)
##stream, addr = servsock.accept()
##mapper1()

def mapper1():
    while True:
        try:
##            data = stream.recv(1024)
##            streamData = data.split()
            streamData = ['map,file1.txt,0,200','map,file2.txt,0,200']
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
            break
        except KeyboardInterrupt:
            break

mapper1()

    
