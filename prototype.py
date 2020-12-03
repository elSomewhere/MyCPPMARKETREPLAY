import csv
import socket
import pandas as pd
from array import array
import numpy as np
import time
import threading
from queue import Queue
from functools import partial
np.set_printoptions(suppress=True)


# threads A & B run simultanesously unless buffer is empty of full.
# if buffer is empty, B pauses.
# Note: This is "unlikely" since B is timed and would usually sleep if it were executing too quickly.
# But it can happen if there is a giant burst in A and it churns while buffer emties alongside. if it happens, all computing goes to A.
# if buffer is full, A pauses and just B does its job.
# if A is thru & buffer is empty, B can stop.
# this is producer / consumer patterns with a 
# we need a threadsafe fifo queue or ringbuffer


def parselinefun_standardjoin(packet_data):
    return '/'.join(map(str, packet_data))

def sendfun_dummy(message):
    print("sending %s" % message)

def sendfun_tcp(socket, message_bytes):
    print("sending tcp %s" % message_bytes)
    socket.sendall(message_bytes)
    data = socket.recv(65535)
    #print("received ", data)

def parsechunkfun_dummy(chunk):
    var = ';'.join(map(str, chunk))
    return var

def parsechunkfun_bytes(chunk):
    var = ';'.join(map(str, chunk))
    var = var+'\n'
    var_bytes = array('b')
    var_bytes.frombytes(var.encode())
    return var_bytes

class Message():
    def __init__(self, chunk, timer):
        self.chunk_bytes = chunk
        self.timer = timer

class Buffer():
    def __init__(self, global_start_time, first_bytes, max_buffer_size):
        self.tail_time = global_start_time
        self.queue = Queue(maxsize=max_buffer_size) # fifo
        self.queue.put(Message(first_bytes, 0))

    def put_message(self, bytes, last_timestamp):
        delta = (pd.to_datetime(last_timestamp) - pd.to_datetime(self.tail_time)).total_seconds() * 1000
        message = Message(bytes, delta)
        self.tail_time = last_timestamp
        self.queue.put(message)

def parse_raw_line(packet_data, chunk, max_chunk_size, timestamp_id, last_timestamp, buffer, parsechunkfun, parselinefun):
    timestamp_delta = (pd.to_datetime(packet_data[timestamp_id])-pd.to_datetime(last_timestamp)).total_seconds() * 1000
    # acumulate
    if timestamp_delta == 0 and len(chunk) < max_chunk_size:
        #print("accumulate")
        chunk += [parselinefun(packet_data)]
        return chunk, last_timestamp
    # max chunk reached, but still in same timestamp
    elif timestamp_delta == 0 and len(chunk) >= max_chunk_size:
        #print("max chunk reached, but still in same timestamp")
        message = parsechunkfun(chunk)
        buffer.put_message(message, last_timestamp)
        chunk = [parselinefun(packet_data)]
        return chunk, last_timestamp
    # new time step reached, send out
    elif timestamp_delta > 0 and len(chunk) < max_chunk_size:
        #print("new time step reached, send to buffer.")
        message = parsechunkfun(chunk)
        buffer.put_message(message, last_timestamp)
        last_timestamp = packet_data[timestamp_id]
        chunk = [parselinefun(packet_data)]
        return chunk, last_timestamp
    # new timestep reached and
    elif timestamp_delta > 0 and len(chunk) >= max_chunk_size:
        #print("new timestep reached and max chunk size reached")
        message = parsechunkfun(chunk)
        buffer.put_message(message, last_timestamp)
        last_timestamp = packet_data[timestamp_id]
        chunk = [parselinefun(packet_data)]
        return chunk, last_timestamp




def readlines(reader, timestamp_id, max_chunk_size, buffer, event_alive, event_buffer_data_empty, event_buffer_data_full, parsechunkfun, parselinefun):
    # if buffer full, stop
    chunk = []
    packet_data = next(reader)
    last_timestamp = packet_data[timestamp_id]
    while(event_alive.is_set()): # proxy for data available
        #print("is alive")
        if not buffer.queue.full():
            event_buffer_data_full.clear()
            #time.sleep(1)
            #print("worker A is working")
            chunk, last_timestamp = parse_raw_line(packet_data, chunk, max_chunk_size, timestamp_id, last_timestamp, buffer, parsechunkfun, parselinefun)
            packet_data = next(reader)
            if(len(packet_data)==0):
                event_alive.clear()
            continue
        elif buffer.queue.full():
            event_buffer_data_full.set()
            event_buffer_data_empty.clear()
            #print("worker A is idling")
            #time.sleep(1)
            if (len(packet_data) == 0):
                event_alive.clear()
            continue
        elif buffer.queue.empty():
            event_buffer_data_full.clear()
            event_buffer_data_empty.set()
    return True


def sendlines(buffer, event_buffer_data_empty, event_buffer_data_full, sendfun, time_scale=1.0):
    # if buffer is empty, stop
    time_at_last_sent_message = -np.inf
    while(True):
        if not buffer.queue.empty():
            event_buffer_data_empty.clear()
            #print("worker B is working")
            #time.sleep(1)
            next_message = buffer.queue.get()
            next_message_delta = next_message.timer
            next_message_chunk_bytes = next_message.chunk_bytes
            wait_for = np.max(next_message_delta - (time.time() - time_at_last_sent_message), 0)
            if wait_for > 0:
                #print("waiting for %s" % wait_for)
                time.sleep(wait_for/1000*time_scale)
            sendfun(next_message_chunk_bytes)
            time_at_last_sent_message = time.time()
            #print("buffer siez is %s" % buffer.queue.qsize())
            continue
        elif buffer.queue.empty():
            #print("worker B is idling")
            #time.sleep(1)
            event_buffer_data_empty.set()
            continue
        elif buffer.queue.full():
            #print("worker B is idling")
            #time.sleep(1)
            event_buffer_data_full.set()
            continue
        print("sendlines thru")




def replay(csv_path, timestamp_id, max_chunk_size, max_buffer_size, time_scale, sendfun, parsechunkfun, parselinefun):
    # the incremental time step size for the simulation
    my_csv = open(csv_path, "r")
    reader = csv.reader(my_csv)
    # get rid of the header
    header = next(reader)
    # get the first line to initialize
    first_line = next(reader)
    # get the first timestamp
    init_timestamp = pd.to_datetime(first_line[timestamp_id])
    # construct an initalizer message
    first_message_chunk = [parselinefun("start/"+str(init_timestamp)+";")]
    init_bytes = parsechunkfun(first_message_chunk)
    buffer = Buffer(init_timestamp, init_bytes, max_buffer_size)
    # re-initalize reader to actually start operating
    reader = csv.reader(my_csv)
    # get rid of the header
    header = next(reader)
    # buffer is initalized. Now start the threads
    event_alive = threading.Event()
    event_buffer_data_empty = threading.Event()
    event_buffer_data_full = threading.Event()
    event_alive.set()
    event_buffer_data_empty.clear()
    event_buffer_data_full.clear()
    threading.Thread(target=readlines,args=(reader, timestamp_id, max_chunk_size, buffer, event_alive, event_buffer_data_empty, event_buffer_data_full, parsechunkfun, parselinefun)).start()
    threading.Thread(target=sendlines,args=(buffer,event_buffer_data_empty, event_buffer_data_full, sendfun, time_scale)).start()



if __name__ == "__main__":
    simulate = False
    csv_path = "sample_1000_1030.csv"
    timestamp_id = 0
    max_chunk_size = 100
    max_buffer_size = 100
    serverName = 'localhost'
    serverPort = 2000
    time_scale = 1
    if simulate:
        sendfun = sendfun_dummy
        parsechunkfun = parsechunkfun_dummy
    else:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((serverName, serverPort))  # here is the error
        sendfun = partial(sendfun_tcp, s)
        parsechunkfun = parsechunkfun_bytes
    parselinefun = parselinefun_standardjoin
    replay(csv_path, timestamp_id, max_chunk_size, max_buffer_size, time_scale, sendfun, parsechunkfun, parselinefun)