#! /user/bin/env python3
from dotenv import load_dotenv

import socket, sys, time, os, threading, copy, signal
from collections import defaultdict

# if len(sys.argv) < 2:
#   print('Usage: <side> <user>. Example: python3 testing_02.py sender Taro')
#   exit()
load_dotenv()
# print(os.environ)
# [side, user] = sys.argv[1:]
side = os.environ["side"]
user = os.environ["host"]

host = "" #receiver
bind_host = "" #sender
port = 0
bind_port = 0

Taro = ("169.254.155.219",14547)
Hanako = ("169.254.229.153", 14548)

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

partsPerFile = 100
fileSize = 102400
sizePerPart = 102400 // partsPerFile
indexingSize = 2

threads = 1
filenumber = 270
require = []
current_file = 0
DATA_PATH = "data/data"
raws=[]
  

def read_files():
  for fileno in range(filenumber):
      #read file
      fi = open(DATA_PATH+str(fileno),'rb')
      f = fi.read()
              
      #init
      start = 0
      end = sizePerPart
      for pktno in range(partsPerFile):
          #make packet
          header = (fileno * partsPerFile + pktno).to_bytes(2, 'big')
          raw = header + f[start:end]
          raws[fileno].append(raw)
          #set next packet
          start = end
          end = start + sizePerPart
      #close file
      fi.close()

def work():
  for i in range(filenumber):
    for j in range(partsPerFile):
      s.sendto(raws[i][j], (host, port))
    if i % 10 == 0:
      print(i)

def check_fail(args1, args2):
  recv_binary_data = s2.recv(1000*indexingSize)
 # print("#### resend ####")
  for j in range(0,len(recv_binary_data),indexingSize):
    tmp = int.from_bytes(recv_binary_data[j:j + indexingSize],'big')
    file = tmp//partsPerFile
    part = tmp% partsPerFile
    s.sendto(raws[file][part], (host, port))
  #  print((file,part))
 # print("############\n")


def require_resend_thread():
  INTERRUPT_TIME = 0.4 #check every 0.1s    
  while True:
    time.sleep(INTERRUPT_TIME)
    lock.acquire()
    r = require.copy()
    now = copy.deepcopy(current_file)
    #print("Now --- "+str(now))
    lock.release()
    #print("###resend####")
    raw = b""
    for i in range(now-1, -1 ,-1):
      if not r[i][partsPerFile]:
        continue
      for j in range(partsPerFile):
        if r[i][j] == True:
         # print((i,j))
          raw += (i*partsPerFile + j).to_bytes(2,'big')
    s2.sendto(raw,(bind_host, bind_port))
  #  print("#########\n")

def get_data():
  global require
  global current_file
  count = 0
  while True:
    data = s.recv(sizePerPart + indexingSize)
    index = int.from_bytes(data[:indexingSize], 'big')
    file = index // partsPerFile
    part = index % partsPerFile
    lock.acquire()
    if not require[file][part]:
      lock.release()
      continue
    require[file][part] = False
    if current_file < file:
      current_file = file
    lock.release()
    filePath = 'data/data' + str(file)
    buff[file].append([part, data[indexingSize:]])
    if len(buff[file]) == partsPerFile:
      b = buff[file]
      b.sort(key=lambda x: x[0])
      open(filePath, 'wb').write(b''.join(map(lambda item: item[1], b)))
      count += 1
      print(str(count) + ":   "+ str(file))
      lock.acquire()
      require[file][partsPerFile] = False
      lock.release()
      del buff[file]


if side == 'sender':
  if user == "Taro":
    host, port = Hanako
    bind_host, bind_port = Taro
  elif user == "Hanako":
    filenumber = 10000
    host, port = Taro
    bind_host, bind_port = Hanako
  raws = [[] for _ in range(filenumber)]
  read_files()
  t = threading.Thread(target=work, args=())
  t.start()
  s2.bind((bind_host,bind_port))
  INTERRUPT_TIME = 0.3 #process resend every 0.1s
  signal.signal(signal.SIGALRM, check_fail)
  signal.setitimer(signal.ITIMER_REAL, INTERRUPT_TIME, INTERRUPT_TIME)
else:
  if user == "Taro":
    filenumber = 10000
    host, port = Taro
    bind_host, bind_port = Hanako
  elif user == "Hanako":
    host, port = Hanako
    bind_host, bind_port = Taro
  require = [[True]*(partsPerFile+1) for _ in range(filenumber)]
  s.bind((host, port))
  buff = defaultdict(list)
  lock = threading.Lock()
  t1 = threading.Thread(target=require_resend_thread, args=())
  t2 = threading.Thread(target=get_data, args=())
  t1.start()
  t2.start()
  t1.join()
  t2.join()
  
 