from dotenv import load_dotenv

import socket, sys, time, os, threading, copy, signal
from collections import defaultdict

from mlt_stack import DATA_PATH


load_dotenv()

target = "" #the target address of the server
this_ip = "" #the running machine's address
target_port = 0 #the target port
this_port = 0 #the running machine's port

Taro_IP = "169.254.155.219"
Taro_PORT = 14447
Hanako_IP = "169.254.155.153"
Hanako_PORT = 14547

#============== Custom Config ==============#
initial_file = 0
file_part_size = 40
threads_size = 10

#============== Static Config ==============#
DATA_PREFIX = "data"
DATA_PATH = "data/" + DATA_PREFIX
filesize = os.path.getsize(DATA_PATH + "0") or 102400
part_size = filesize // file_part_size
file_quantity = 1000
header_size = 2

#=============== Debug&Value ===============#
current_file = initial_file
corrupted_file = []
received_files_db = {}
# raw_db = []  # List to store all raw packet (partitioned)


def import_file(start=0, db_size=100):
  raw_datalist = [[] for q in range(file_quantity)]
  for i in range(start, db_size):
    file = open(DATA_PATH+str(i), "rb").read()

    current_byte = 0
    last_byte = part_size

    for packet_id in range(file_part_size):
      packet_id = (i * file_part_size) + packet_id
      raw_header = (packet_id).to_bytes(2, "big")

      raw_body = file[current_byte:last_byte]

      raw_packet = raw_header + raw_body
      raw_datalist[i].append(raw_packet)

      current_byte = last_byte
      last_byte += part_size
  return raw_datalist      


def send_data(db, port_offset):
  global current_file
  sub_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  for packets in db:
    for packet in packets:
      sub_socket.sendto(packet, (target, target_port + port_offset))
    current_file += 1

# def add_check_corrupted(sub_socket):
#   recv_data = sub_socket.recv(2000)


def recv_data(port_offset):
  sub_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sub_socket.bind((host, port + port_offset))
  global corrupted_file
  global current_file
  count = 0

  while True:
    data = sub_socket.recv(part_size + header_size)
    raw_header = data[:header_size]
    packet_id = int.from_bytes(raw_header, "big")
    file_id = packet_id // file_part_size
    file_part = packet_id % file_part_size

    # thread_locker.acquire()

    corrupted_file[file_id][file_part] = False

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
      del received_files_db[file_id]

side = os.environ["side"]
user = os.environ["host"]
TARO = os.environ["TARO"]
HANAKO = os.environ["HANAKO"]

if side == "send":
  if user == "Taro":
    target = Hanako_IP
    this_ip = Taro_IP
  elif user == "Hanako":
    target = TARO
    this_ip = HANAKO
else:
  if user == "Taro":
    target = TARO
    this_ip = HANAKO
  elif user == "Hanako":
    target = HANAKO
    this_ip = TARO

if side == "send":
  threads_list = {}
  db_size = file_quantity / threads_size
  for thread_id in range(threads_size):
    start = thread_id * db_size
    db = import_file(start, db_size)
    thread = threading.Thread(target=send_data, args=(db, thread_id))
    thread.start()
    threads_list[thread_id] = thread



