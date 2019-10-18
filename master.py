#!/usr/bin/env python
"""
The master program for CS5414 three phase commit project.
"""

import os
import signal
import subprocess
import sys
import time
import traceback
from socket import SOCK_STREAM, socket, AF_INET
from threading import Thread

leader = -1  # coordinator
address = 'localhost'
threads = {}
live_list = {}
crash_later = []
wait_ack = False


class ClientHandler(Thread):
    def __init__(self, index, address, port, process):
        Thread.__init__(self)
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((address, port))
        self.buffer = ""
        self.valid = True
        self.process = process

    def handleMsg(self, msg):
        global wait_ack, leader
        s = msg.split()
        if len(s) < 2:
          sys.stderr("Incorrect Msg Format")
          sys.stderr.flush()
        if s[0] == 'coordinator':
          leader = int(s[1])
          wait_ack = False
        elif s[0] == 'resp':
          sys.stdout.write(s[1] + '\n')
          sys.stdout.flush()
          wait_ack = False
        elif s[0] == 'ack':
          wait_ack = False
        else:
          print s


    def run(self):
        global leader, threads, wait_ack
        msgLen = 0
        while self.valid:
            try:
                # No message is being processed
                if (msgLen == 0):
                    if (len(self.buffer) < 4):
                        # Not enough for an integer,
                        # must read more
                        data = self.sock.recv(1024)
                        self.buffer+=data
                    else:
                        # Now extract header
                        (header,msg)= self.buffer.split("-",1)
                        msgLen = int(header)
                        self.buffer = msg
                else:
                    # Message has not yet been fully received
                    if (len(self.buffer)<msgLen):
                        data = self.sock.recv(1024)
                        self.buffer+=data
                    else:
                        msg,rest = self.buffer[0:msgLen],self.buffer[msgLen:]
                        self.buffer= rest
                        self.handleMsg(msg)
                        msgLen = 0
            except Exception,e:
                print (traceback.format_exc())
                self.valid = False
                del threads[self.index]
                self.sock.close()
                break


    def kill(self):
        if self.valid:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except:
                pass
            self.close()

    def send(self, s):
        if self.valid:
            self.sock.send(str(s) + '\n')
        else:
            print "Socket invalid"

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass


def send(index, data, set_wait_ack=False):
    global leader, live_list, threads, wait_ack
    wait = wait_ack
    while wait:
        time.sleep(0.01)
        wait = wait_ack
    pid = int(index)
    if pid >= 0:
        if pid not in threads:
            print 'Master or testcase error!'
            return
        if set_wait_ack:
            wait_ack = True
        threads[pid].send(data)
        return
    pid = leader
    while pid not in live_list or live_list[pid] == False:
        time.sleep(0.01)
        pid = leader
    if set_wait_ack:
        wait_ack = True
    threads[pid].send(data)


def exit(exit=False):
    global threads, wait_ack

    wait = wait_ack
    wait = wait and (not exit)
    while wait:
        time.sleep(0.01)
        wait = wait_ack

    time.sleep(2)
    for k in threads:
        threads[k].kill()
    subprocess.Popen(['./stopall'], stdout=open('/dev/null'), stderr=open('/dev/null'))
    time.sleep(0.1)
    os._exit(0)


def timeout():
    global wait_ack
    time.sleep(120)
    exit(True)


def main(debug=False):
    global leader, threads, crash_later, wait_ack
    timeout_thread = Thread(target=timeout, args=())
    timeout_thread.setDaemon(True)
    timeout_thread.start()

    while True:
        line = ''
        try:
            line = sys.stdin.readline()
        except:  # keyboard exception, such as Ctrl+C/D
            exit(True)
        if line == '':  # end of a file
            exit()
        line = line.strip()  # remove trailing '\n'
        if line == 'exit':  # exit when reading 'exit' command
            exit()

        sp1 = line.split(None, 1)
        sp2 = line.split()
        if len(sp1) != 2:  # validate input
            print "Invalid command: " + line
            exit(True)

        try:
            pid = int(sp2[0])  # first field is pid
        except ValueError:
            print "Invalid pid: " + sp2[0]
            exit(True)

        cmd = sp2[1]  # second field is command
        if cmd == 'start':
            port = int(sp2[3])
            # if no leader is assigned, set the first process as the leader
            if leader == -1:
                leader = pid
            live_list[pid] = True

            if debug:
                process = subprocess.Popen(['./process', str(pid), sp2[2], sp2[3]], preexec_fn=os.setsid)
            else:
                process = subprocess.Popen(['./process', str(pid), sp2[2], sp2[3]], stdout=open('/dev/null', 'w'),
                    stderr=open('/dev/null', 'w'), preexec_fn=os.setsid)

            # sleep for a while to allow the process be ready
            time.sleep(3)

            # connect to the port of the pid
            handler = ClientHandler(pid, address, port, process)
            threads[pid] = handler
            handler.start()
            # sleep for a while to allow the coordinator inform the master
            time.sleep(0.1)
        elif cmd == 'get':
            send(pid, sp1[1], set_wait_ack=True)
        elif cmd == 'add' or cmd == 'delete':
            send(pid, sp1[1], set_wait_ack=True)
            for c in crash_later:
                live_list[c] = False
            crash_later = []
        elif cmd == 'crash':
            send(pid, sp1[1])
            if pid == -1:
                pid = leader
            live_list[pid] = False
        elif cmd[:5] == 'crash':
            send(pid, sp1[1])
            if pid == -1:
                pid = leader
            crash_later.append(pid)
        elif cmd == 'vote':
            send(pid, sp1[1])
        time.sleep(2)


if __name__ == '__main__':
    debug = False
    if len(sys.argv) > 1 and sys.argv[1] == 'debug':
        debug = True

    main(debug)
