from queue import Queue
import socket
import threading
from enum import Enum

from packet import SCUPacketType, SCUHeader, SCUPacket
import utils

class SCUMode(Enum):
    SendMode = 0
    RecvMode = 1

class SCU:
    def __init__(self, mtu=1500):
        self.mtu = mtu

    def bind_as_sender(self, receiver_address):
        self.mode = SCUMode.SendMode
        self.connection_manager = {}

        self.socket =  socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiver_address = receiver_address
        self.lock = threading.Lock()

        sender_packet_loop_thread = threading.Thread(target=self._sender_packet_loop)
        sender_packet_loop_thread.setDaemon(True)
        sender_packet_loop_thread.start()

    def bind_as_receiver(self, receiver_address):
        self.mode = SCUMode.RecvMode
        self.received_files_data = {}
        self.lost_packets_recv = {}      #save the seq data of un-received packets

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(receiver_address)

        self.file_received = Queue()

        receiver_packet_loop_thread = threading.Thread(target=self._receiver_packet_loop)
        receiver_packet_loop_thread.setDaemon(True)
        receiver_packet_loop_thread.start()

    def drop(self):
        if self.mode == SCUMode.SendMode:
            self.connection_manager.clear()
            self.lost_packets_send.clear()
            self.socket.close()

    def _sender_packet_loop(self):
        if self.mode == SCUMode.RecvMode:
            raise Exception
        while True:
            try:
                packet = SCUPacket()
                packet.from_raw(self.socket.recv(2048))
                if packet.header.id not in self.connection_manager:
                    continue
                if packet.header.typ == SCUPacketType.Fin.value:
                    self.connection_manager[packet.header.id].put((True, packet.header.seq, None, None))
                elif packet.header.typ == SCUPacketType.Rtr.value:
                    lost, last_seq = self.parse_rtr_list(packet.payload)
                    self.connection_manager[packet.header.id].put((False, packet.header.seq, lost, last_seq))
            except Exception as e: # When recv fails and when put fails (appropriate)
                if e == KeyboardInterrupt:
                    raise KeyboardInterrupt
                else:
                    import traceback
                    traceback.print_exc()

    def send(self, filepath, id): # will lock the thread
        if self.mode == SCUMode.RecvMode:
            raise Exception
        queue = Queue()
        self.connection_manager[id] = queue # Register a connection

        data_fragments = utils.split_file_into_mtu(filepath, self.mtu)

        all_packets = []
        for (seq, df) in enumerate(data_fragments):
            # create header
            header = SCUHeader()
            if seq == len(data_fragments) - 1:
                header.from_dict({ "typ": SCUPacketType.DataEnd.value, "id": id, "seq": seq, })
            else:
                header.from_dict({ "typ": SCUPacketType.Data.value, "id": id, "seq": seq, })
            # create packet
            packet = SCUPacket()
            packet.from_dict({ "header": header, "payload": df, })

            all_packets.append(packet)

        retransmit_seq = 0 # Manage packets that need to be retransmitted (how far you can receive)
        seqPos = 0
        max_last_seq = 0
        lost_packets_send = [i for i in range(len(all_packets))]
        while True:
            try:
                while True:
                    try:
                        fin, sq, lost, last_seq = queue.get(block=False) # Resend request or reception completion report
                        if fin: # send completely
                            del(self.connection_manager[id]) # Disconnect
                            return
                        elif sq < len(all_packets): # Retransmission request
                            retransmit_seq = max(sq, retransmit_seq)
                            if (retransmit_seq == sq):
                                lost_packets_send = lost
                                #seqPos = 0
                            if (last_seq > max_last_seq):
                                max_last_seq = last_seq
                                lost_packets_send = lost

                    except Exception as e: # When the queue is empty
                        if e == KeyboardInterrupt:
                            raise KeyboardInterrupt
                        else:
                            break
                with self.lock: # Lock required as multiple send methods may be running concurrently in parallel
                    self.socket.sendto(all_packets[lost_packets_send[seqPos % len(lost_packets_send)]].raw(), self.receiver_address) # Packet transmission
                    if (max_last_seq != len(all_packets)):
                        self.socket.sendto(all_packets[-1].raw(), self.receiver_address)

                #seq = max(seq + 1, retransmit_seq) # seq update
                seqPos += 1

            except Exception as e: # When sendto fails (appropriate)
                if e == KeyboardInterrupt:
                    raise KeyboardInterrupt
                else:
                    import traceback
                    traceback.print_exc()



    def _receiver_packet_loop(self):
        if self.mode == SCUMode.SendMode:
            raise Exception
        received_files_flag = {}
        received_files_length = {}
        while True:
            try:
                data, from_addr = self.socket.recvfrom(2048)
                packet = SCUPacket()
                packet.from_raw(data)

                key = utils.endpoint2str(from_addr, packet.header.id)
                if key not in self.received_files_data:
                    self.received_files_data[key] = [b""]*100
                    received_files_flag[key] = False
                    self.lost_packets_recv[key] = [] * 100

                if received_files_flag[key]:
                    self.response(SCUPacketType.Fin.value, from_addr, packet.header.id, 0)
                    continue

                if packet.header.typ == SCUPacketType.DataEnd.value or packet.header.typ == SCUPacketType.Data.value:
                    if packet.header.typ == SCUPacketType.DataEnd.value:
                        received_files_length[key] = packet.header.seq + 1

                    self.received_files_data[key][packet.header.seq] = packet.payload
                    rtr = self.calculate_rtr(key, packet.header.seq)
                    if rtr is not None: # Need to request resend
                        self.response(SCUPacketType.Rtr.value, from_addr, packet.header.id, rtr, key, packet.header.seq)
                    elif key in received_files_length and self.is_all_received(key, received_files_length[key]): #  File reception completed
                        received_files_flag[key] = True
                        self.response(SCUPacketType.Fin.value, from_addr, packet.header.id, 0)
                        self.file_received.put((key, received_files_length[key]))

            except Exception as e: # When recv fails and when put fails (appropriate)
                if e == KeyboardInterrupt:
                    raise KeyboardInterrupt
                else:
                    import traceback
                    traceback.print_exc()

    def calculate_rtr(self, key, seq):
        for sq in range(0, seq):
            if not self.received_files_data[key][sq]:
                self.lost_packets_recv[key].append(sq)

        if (len(self.lost_packets_recv[key]) == 0):
            return None
        else:
            return self.lost_packets_recv[key][0]

    def is_all_received(self, key, length):
        for i in range(0, length):
            if not self.received_files_data[key][i]:
                return False
        return True

    def response(self, typ, addr, id, rtr, key = None, lastSeq = None):
        if self.mode == SCUMode.SendMode:
            raise Exception
        if typ == SCUPacketType.Rtr.value:
            header = SCUHeader()
            header.from_dict({ "typ": typ, "id": id, "seq": rtr, })
            packet = SCUPacket()
            payload = self.encode_rtr_list(key, lastSeq)
            packet.from_dict({ "header": header, "payload": payload, })
            self.socket.sendto(packet.raw(), addr)

        elif typ == SCUPacketType.Fin.value:
            header = SCUHeader()
            header.from_dict({ "typ": typ, "id": id, "seq": rtr, })
            packet = SCUPacket()
            packet.from_dict({ "header": header, "payload": b'', })
            self.socket.sendto(packet.raw(), addr)

    def recv(self):
        if self.mode == SCUMode.SendMode:
            raise Exception
        key, length = self.file_received.get()
        return utils.fold_data(self.received_files_data[key], length)


    def encode_rtr_list(self, key, last_seq):
        data = b""
        for i in range(len(self.lost_packets_recv[key])):
            data += self.lost_packets_recv[key].pop(-1).to_bytes(1, "big")
        data += last_seq.to_bytes(1, "big")
        return data

    def parse_rtr_list(self, payload):
        lost_packets = []
        for i in range(len(payload) - 1):
            lost_packets.append(payload[i])
        last_seq = payload[-1]
        return lost_packets, last_seq
