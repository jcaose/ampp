 # -*- coding: utf-8 -*-
import time, sys, re, struct
import tornado
import ampp


MID_HELLO_LOOP = 100
MID_HELLO_STAT = 200
MID_EXIT = 0


class Stat(object):
    def __init__(self):
        self.count = 0
        self.bytes = 0
        self.tic = time.time()

    def update(self, count, bytes):
        self.count += count
        self.bytes += bytes


def main():
    from  multiprocessing import Process
    def run_master():
        print "master starts"
        ioloop = tornado.ioloop.IOLoop()
        master = ampp.AmppTcpMaster(8000, ioloop = ioloop)
        master.start()


        stat = Stat()

        def on_slave_connect(slave_conn):

            def message_callback(mid, body):

                if mid == MID_HELLO_LOOP :
                    #stat.update(1, 1024*64)
                    slave_conn.recv(MID_HELLO_LOOP, message_callback)
                elif mid == MID_HELLO_STAT:

                    count, bytes =  struct.unpack('II', body)
                    stat.update(count, bytes)

                    tac = time.time()
                    t = tac - stat.tic
                    count = stat.count
                    bytes = stat.bytes
                    print "total time=", t, " ",  \
                          "# of messages= ",  count, " ", \
                          "# of bytes= ", bytes, " "
                    print  "delay µs= ", t/count*1e6, " ", \
                           "throughput kmsg/s=", count/t/1e3, " ", \
                          "throughput MB/s",  bytes/t/(1024*1024)
                    slave_conn.recv(MID_HELLO_LOOP, message_callback)
                    slave_conn.recv(MID_HELLO_STAT,message_callback)

                elif mid == MID_EXIT:
                    ioloop.stop()


            slave_conn.recv( MID_HELLO_LOOP, message_callback)
            slave_conn.recv( MID_HELLO_STAT, message_callback)
            slave_conn.recv( MID_EXIT, message_callback)


        master.set_connect_callback(re.compile(r"slave\d+"), on_slave_connect)

        ioloop.start()


    def run_slave():
        print "slave  starts"
        ioloop = tornado.ioloop.IOLoop.instance()

        def start():
            def on_close(slave):
                print "slave closing down as master closed the connection"
                ioloop.stop()

            def on_connect(slave):
                print "start sending"

                def send():
                    for i in xrange(8192):
                        slave.send(MID_HELLO_LOOP, body='*'*1024*64 )
                    slave.send(MID_HELLO_STAT, struct.pack('II', 8192,8192*1024*64))

                def on_tx_queue_empty():
                    print "*******"
                    send()
                    slave.set_tx_queue_callback(0, on_tx_queue_empty)

                slave.set_tx_queue_callback(0, on_tx_queue_empty)

                send()


                #slave.send(MID_EXIT)
                print "sending completed"



            ampp.AmppTcpSlave("slave0", "127.0.0.1", 8000,
                ioloop = ioloop,
                connect_callback = on_connect,
                close_callback = on_close )

        ioloop.add_callback(start)
        ioloop.start()

    master = Process(target = run_master )
    slave = Process(target = run_slave)
    try:
        master.start()
        slave.start()

        master.join()
        slave.join()
    except KeyboardInterrupt:
        master.terminate()
        slave.terminate()
        sys.exit()

import logging
logging.basicConfig()

main()


