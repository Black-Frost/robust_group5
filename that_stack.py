from dotenv import load_dotenv

import socket, sys, time, os, threading, copy, signal
from collections import defaultdict

# from mlt_stack import DATA_PATH


load_dotenv()

receiver_ip = "" #the target address of the server
sender_ip = "" #the running machine's address
receiver_port = 0 #the target port
sender_port = 0 #the running machine's port

Taro_IP = "169.254.155.219"
# Taro_PORT = 24447
Hanako_IP = "169.254.229.153"
# Hanako_PORT = 24547

#============== Custom Config ==============#
initial_file = 0
file_part_size = 40
threads_size = 10
sender_port = 24680
receiver_port = 24656

#============== Static Config ==============#
DATA_PREFIX = "data"
DATA_PATH = "data/" + DATA_PREFIX
# filesize = os.path.getsize(DATA_PATH + "0") or 102400
filesize = 102400
part_size = filesize // file_part_size
file_quantity = 1000
header_size = 2
packets_list = [i for i in range(file_part_size * file_quantity)]

#=============== Debug&Value ===============#
current_file = initial_file
fail_packets = []
current_packet = 0
corrupted_file = [[True]*(file_part_size+1) for i in range(file_quantity)]
received_files_db = {}
# raw_db = []  # List to store all raw packet (partitioned)


def import_file(start=0, db_size=100):
  print("Importing data")
  raw_datalist = [[] for q in range(file_quantity)]
  for i in range(start, db_size):
    file = open(DATA_PATH+str(i), "rb").read()

    current_byte = 0
    last_byte = part_size

    for packet_id in range(file_part_size):
      packet_id = (i * file_part_size) + packet_id
      raw_header = (packet_id).to_bytes(header_size, "big")

      raw_body = file[current_byte:last_byte]

      raw_packet = raw_header + raw_body
      raw_datalist[i].append(raw_packet)

      current_byte = last_byte
      last_byte += part_size
  print("Import Done")
  return raw_datalist      


def send_data(db, port_offset=0):
  global current_file
  sub_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  for packets in db:
    for packet in packets:
      sub_socket.sendto(packet, (receiver_ip, receiver_port + port_offset))
    current_file += 1
    if current_file % 5 ==0:
      print("send:",current_file)



# def add_check_corrupted(sub_socket):
#   recv_data = sub_socket.recv(2000)

def request_lost_packet(sub_socket, packet_id, port_offset=0):
  raw_packet_id = (packet_id).to_bytes(packet_id, "big")
  print("request resend packet id:", packet_id)
  sub_socket.sendto(raw_packet_id, (sender_ip, sender_port + port_offset))

def check_lost_packet():
  sub_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  while True:
    for packet in packets_list:
      if packet <= current_packet:
        request_lost_packet(sub_socket, packet)
  print("stop checking")
        
def listen_lost_packet():
  global sender_ip
  sub_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sub_socket.bind((sender_ip, sender_port))
  while True:
    data = sub_socket.recv(header_size)
    packet_id = int.from_bytes(data, "big")
    print("recv fail,", packet_id)
    print("fail_packets", fail_packets)
    if packet_id not in fail_packets:
      print('recv fail_packets req', packet_id)
      fail_packets.append(packet_id)



def resend_failed_packets(db, port_offset=0):
  
  sub_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  while True:
    if len(fail_packets) > 0:
      # lock.acquire()
      packet_id = fail_packets[0]
      file = header_size // file_part_size
      part = header_size % file_part_size
      packet = db[file][part]
      print("resend_failed_packets", packet_id)
      sub_socket.sendto(packet, (receiver_ip, receiver_port + port_offset))
      fail_packets.remove(packet_id)
      # lock.release()

def check_fail(_1,_2):
  global db
  data = fail_socket.recv(1000*header_size)
 # print("#### resend ####")
  for i in range(0,len(data),header_size):
    packet_id = int.from_bytes(data[i: (i + header_size)],'big')
    file = packet_id // file_part_size
    part = packet_id % file_part_size
    send_socket.sendto(db[file][part], (receiver_ip, receiver_port))
  #  print((file,part))
 # print("############\n")


def require_resend_thread():
  INTERRUPT_TIME = 0.4 #check every 0.1s    
  sub_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  while True:
    time.sleep(INTERRUPT_TIME)
    lock.acquire()
    r = corrupted_file.copy()
    snap_current = copy.deepcopy(current_file)
    lock.release()
    raw = b""
    for file_id in range(snap_current-1, -1 ,-1):
      if not r[file_id][file_part_size]:
        continue
      for file_part in range(file_part_size):
        if r[file_id][file_part] == True:
          raw += (file_id*file_part_size + file_part).to_bytes(2,'big')
    print("send req resent", )
    sub_socket.sendto(raw,(sender_ip, sender_port))


def recv_data(port_offset=0):
  global corrupted_file
  global current_file
  count = 0

  while True:
    
    data = recv_socket.recv(part_size + header_size)
    raw_header = data[:header_size]
    packet_id = int.from_bytes(raw_header, "big")
    file_id = packet_id // file_part_size
    file_part = packet_id % file_part_size
    
    print("recv:", packet_id, file_id, file_part)
    lock.acquire()
    if not corrupted_file[file_id][file_part]:
      lock.release()
      continue


    # thread_locker.acquire()
    # if packet_id not in packets_list:
    #   continue
    # packets_list.remove(packet_id)
    corrupted_file[file_id][file_part] = False
    current_file = file_id
    lock.release()
    received_files_db.setdefault(file_id, [])
    received_files_db[file_id].append([file_part, data])
    if len(received_files_db[file_id]) == file_part_size:
      write_path = DATA_PATH + str(file_id)
      raw_data_db = received_files_db[file_id]
      raw_data_db.sort(key=lambda x: x[0])
      raw_data_list = map(lambda part: part[1], raw_data_db)
      raw_data = b''.join(raw_data_list)
      open(write_path, "wb").write(raw_data)
      count += 1
      
      print("Writing file_id:", file_id)
      lock.acquire()
      corrupted_file[file_id][file_part_size] = False
      lock.release()
      del received_files_db[file_id]
      # print("writing :",file_id)

side = os.environ["side"]
user = os.environ["host"]
TARO = os.environ["TARO"]
HANAKO = os.environ["HANAKO"]

if side == "send":
  if user == "Taro":
    receiver_ip = HANAKO
    sender_ip = TARO
  elif user == "Hanako":
    receiver_ip = TARO
    sender_ip = HANAKO
else:
  if user == "Taro":
    receiver_ip = TARO
    sender_ip = HANAKO
  elif user == "Hanako":
    receiver_ip = HANAKO
    sender_ip = TARO

# if (side == "send" and user == "Hanako") or (side == "recv" and user == "Taro"):
#   receiver_ip = TARO
#   sender_ip = HANAKO
# else:
#   receiver_ip = HANAKO
#   sender_ip = TARO


# if side == "send":
#   threads_list = {}
#   db_size = file_quantity / threads_size
#   for thread_id in range(threads_size):
#     start = thread_id * db_size
#     db = import_file(start, db_size)
#     thread = threading.Thread(target=send_data, args=(db, thread_id))
#     thread.start()
#     threads_list[thread_id] = thread

lock = threading.Lock()
# if side == "send":
#   db = import_file(0, 1000)
#   # send_data(db)
#   send_thread = threading.Thread(target=send_data, args=((db),0))
#   listen_lost_thread = threading.Thread(target=listen_lost_packet)
#   resend_thread = threading.Thread(target=resend_failed_packets, args=((db),0))
#   listen_lost_thread.start()
#   send_thread.start()
#   resend_thread.start()
#   listen_lost_thread.join()
#   send_thread.join()
#   resend_thread.join()
# else:
#   # recv_data()
#   receive_thread = threading.Thread(target=recv_data, args=())
#   req_resend_thread = threading.Thread(target=check_lost_packet, args=())
#   receive_thread.start()
#   req_resend_thread.start()
#   receive_thread.join()
#   req_resend_thread.join()


if side == "send":
  db = import_file(0, 1000)
  fail_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  # print((sender_ip, sender_port))
  fail_socket.bind((sender_ip, sender_port))
  send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sender_thread = threading.Thread(target=send_data, args=((db),0))
  INTERRUPT_TIME = 0.3 #process resend every 0.1s
  sender_thread.start()
  signal.signal(signal.SIGALRM, check_fail)
  signal.setitimer(signal.ITIMER_REAL, INTERRUPT_TIME, INTERRUPT_TIME)
else:
  recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  recv_socket.bind((receiver_ip, receiver_port))
  receive_thread = threading.Thread(target=recv_data, args=())
  req_resend_thread = threading.Thread(target=require_resend_thread, args=())
  receive_thread.start()
  req_resend_thread.start()
  receive_thread.join()
  req_resend_thread.join()