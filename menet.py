import os
from dotenv import load_dotenv
import random
import types

import enet

load_dotenv()
side = os.environ["side"]
addr = bytes(os.environ["TARO"],"utf-8")



if side == "recv":
  host = enet.Host(enet.Address(addr,8899),8,8)
  
  while True: 
    event = host.service(1000)
    if event.type == "ENET_EVENT_TYPE_CONNECT ":
      print("new client", event.peer)
    
    elif event.type == "ENET_EVENT_TYPE_RECEIVE":
      print("data recv")
      print(event.packet)
      print(event.peer)
      print(event.channelID)


    elif event.type == "ENET_EVENT_TYPE_DISCONNECT":
      print("client discon", event.peer)

elif side == "send":
  counter = 0
  SHUTDOWN_MSG = b"SHUTDOWN"
  MSG_NUMBER = 100
  host = enet.Host(None, 1, 0, 0, 0)
  peer = host.connect(enet.Address(addr, 8899), 1)
  while True:
    event = host.service(1000)
    if event.type == enet.EVENT_TYPE_CONNECT:
        print("%s: CONNECT" % event.peer.address)
    elif event.type == enet.EVENT_TYPE_DISCONNECT:
        print("%s: DISCONNECT" % event.peer.address)
        run = False
        continue
    elif event.type == enet.EVENT_TYPE_RECEIVE:
        print("%s: IN:  %r" % (event.peer.address, event.packet.data))
        continue
    msg = bytes("test", "utf-8")
    packet = enet.Packet(msg)
    peer.send(0, packet)

    counter += 1
    if counter >= MSG_NUMBER:
        msg = SHUTDOWN_MSG
        peer.send(0, enet.Packet(msg))
        host.service(100)
        peer.disconnect()

    print("%s: OUT: %r" % (peer.address, msg))