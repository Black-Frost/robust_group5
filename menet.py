import os
from dotenv import load_dotenv
import random
import types

import enet

load_dotenv()
side = os.environ["side"]
print(os.environ)
addr = bytes(os.environ["HANAKO"],"utf-8")



if side == "recv":
  host = enet.Host(enet.Address(addr,8899),10,8,0,0)
  connect_count = 0
  run = True
  shutdown_recv = False
  while True: 
    event = host.service(10)
    if event.type == enet.EVENT_TYPE_CONNECT:
        print("%s: CONNECT" % event.peer.address)
        connect_count += 1
    elif event.type == enet.EVENT_TYPE_DISCONNECT:
        print("%s: DISCONNECT" % event.peer.address)
        connect_count -= 1
        if connect_count <= 0 and shutdown_recv:
            run = False
    elif event.type == enet.EVENT_TYPE_RECEIVE:
        print("%s: IN:  %r" % (event.peer.address, event.packet.data))
        msg = event.packet.data
        if event.peer.send(0, enet.Packet(msg)) < 0:
            print("%s: Error sending echo packet!" % event.peer.address)
        else:
            print("%s: OUT: %r" % (event.peer.address, msg))
        if event.packet.data == b"SHUTDOWN":
            shutdown_recv = True

elif side == "send":
  counter = 0
  SHUTDOWN_MSG = b"SHUTDOWN"
  MSG_NUMBER = 100
  host = enet.Host(None, 1, 0, 0, 0)
  peer = host.connect(enet.Address(addr, 8899), 1)
  run = True
  datalist = [i for i in range(1000)]
  ind = 0
  while run:
    print("=======================================current len dl", len(datalist))
    if 0 == len(datalist):
      run = False
      break
    else:
      event = host.service(1000)
      if event.type == enet.EVENT_TYPE_CONNECT:
          print("%s: CONNECT" % event.peer.address)
      elif event.type == enet.EVENT_TYPE_DISCONNECT:
          print("%s: DISCONNECT" % event.peer.address)
          run = False
          continue
      elif event.type == enet.EVENT_TYPE_RECEIVE:
          print("%s: IN:  %r" % (event.peer.address, event.packet.data))
          if int(event.packet.data) in datalist:
            datalist.remove(int(event.packet.data))
          continue
      data = datalist[ind]
      raw_msg = data
      msg = bytes(str(raw_msg), "utf-8")
      packet = enet.Packet(msg)
      peer.send(0, packet)
      ind += 1
      if ind >= len(datalist):
        ind = 0
      # counter += 1
      # if counter >= MSG_NUMBER:
      #     msg = SHUTDOWN_MSG
      #     peer.send(0, enet.Packet(msg))
      #     host.service(100)
      #     peer.disconnect()

      print("%s: OUT: %r" % (peer.address, msg))