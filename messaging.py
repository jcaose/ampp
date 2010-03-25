import sys, socket, logging, struct, fcntl, errno, time, Queue
from tornado import iostream, ioloop

try:
    import ssl
except ImportError:
    ssl = None

MAGIC = '\x01-\xab^'


''' protocol
- handshake
    slave sends (magic, name) to master
    master replies (magic, name)  with to slave.
  magic is 4 bytes unsigned integer 0x012DAB5E in network (big-endian) order.

- message format
    (mid, prio, size, body),
    mid is 4-byte unsigned in network order.
    prio 1 byte
    size 4 bytes unsigned in network order
    body is empty when size is zero.
'''

''' TODO
 o setup timeout action when degreistion_connection
    timeout and clear master._queues[name] and self._sendings[name]
 o callback when slave connects and disconnects the master
 o callback when master connects and disconnects to the slave
 o rec_always for both master and slave
 o test cases
 o performance suit
 o IPC version
 o pubsub master with/without persistency
 o recv messages with a bitmask
'''


class SendingQueueFull(Exception):
    pass


class PrioFIFOQueue:
    def __init__(self, max_prio=16, max_queue_size=0):
        assert max_prio <= 255
        self._max_prio = max_prio
        self._queues = [ Queue.Queue(max_queue_size) for _ in xrange(self._max_prio+1)]
        self._watermark = -1 #lowest prio of of non empty queue

    def empty(self):
        return self._watermark < 0

    def put_nowait(self, prio, item):
        self._queues[prio].put_nowait(item)

        if self._watermark < 0  or  prio < self._watermark:
            self._watermark = prio

    def get_nowait(self):
        if self.empty():
            raise Queue.Empty
        prio = self._watermark
        item = self._queues[prio].get_nowait()

        self._watermark = -1
        for  p in xrange(prio, self._max_prio+1):
            if not self._queues[p].empty():
                self._watermark = p
                break
        return prio, item


class AmppSlave(object):
    def __init__(self, name):
        pass

    def connect(self):
        pass

    def close(self):
        pass

    def recv(self, mid, callback, timeout=None):
        pass

    def send(self, mid, prio=0, body=None):
        pass


class AmppMaster(object):
    def __init__(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def register_connection(self, connection):
        pass

    def deregister_connection(self, connection):
        pass

    def send(self, dest, mid, prio=0, body=None):
        pass

    def recv(self, src, mid, callback, timeout=None):
        pass

    def dispatch(self, src, mid, prio, body=None):
        pass


class AmppConnection(object):

    def __init__(self, master):
        pass

    def send(self):
        pass

    def recv(self, mid, prio, body=None):
        pass

#----------------------------------------------------
class AmppTcpSlave(object):
    def __init__(self, name, host, port,
                    io_loop = None, ssl_options=None,
                    retry_timeout=0, max_queue_size = 0):
        self.name = name
        self._queue = PrioFIFOQueue(max_queue_size)
        self._callbacks = {}
        self._io_loop = io_loop or ioloop.IOLoop.instance()

        self._ssl_options = ssl_options
        self.host = host
        self.port = port

        self._retry_timeout = retry_timeout
        self._sending = None

        self.connected = False


    def connect(self):
        if not self._connect() and self._retry_timeout:
            self._io_loop.add_timeout(time.time() + self._retry_timeout, self._retry_connect)

    def _connect(self):
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            if self._ssl_options is not None:
                self._socket = ssl.wrap_socket(self._socket, **self._ssl_options)

            self._socket.connect((self.host, self.port))
            self._stream = iostream.IOStream(self._socket, self._io_loop)
            self._stream.write(MAGIC + self.name + '\0')
            self._stream.read_bytes(len(MAGIC), self._verify_magic)

        except:
            logging.error("Error in connecting %s:%d" %(self.host, self.port))
            return False

        return True


    def _verify_magic(self, data):
        if data != MAGIC:
            self._stream.close()
            logging.warning("Master sends wrong magic confirmation")
            return
        self._stream.read_until('\0', self._read_name)


    def _read_name(self, data):
        if data[:-1] != self.name:
            self._stream.close()
            logging.warning("Master sends wrong name confirmation")
            return
        self.connected = True
        self._stream.set_close_callback(self._on_stream_close)
        logging.info("slave (%s) now connects to the master" % self.name)

        self._send()
        self._read_message()


    def _read_message(self):
        self._stream.read_bytes(4, self._read_mid)


    def _retry_connect(self):
        if self.connected:
            return
        if not self._connect():
            self.io_loop.add_timeout(time.time() + self._retry_timeout, self._retry_connect)


    def close(self):
        self._stream.close()
        self.connected = False


    def _on_stream_close(self):
        self.close()
        self.connected = False
        if self._retry_timeout:
            self._retry_connect()


    def recv(self, mid, callback, timeout=None):
        """ receiver message once with timeout.
            TODO: support bitmask on mid
        """
        if mid not in self._callbacks.keys():
            self._callbacks[mid] = []

        assert callable(callback), "callback for Reciver.recvfrom must be callable"

        if timeout:
            def handle_timeout():
                for e in [ (f, _timeout) for (f, _timeout) in self._callbacks[mid]
                        if f==callback and _timeout and timeout.deadline ==_timeout.deadline]:
                    self._callbacks[mid].remove(e)
                if callable(timeout.callback):
                    timeout.callback()

            _timeout = self._io_loop.add_timeout(timeout.deadline, handle_timeout)
        else:
            _timeout = None

        self._callbacks[mid].append((callback, _timeout))


    def _dispatch(self, mid, prio, body=None):
        if mid not in self._callbacks.keys():
            log.warning("no callbacks for mid %08X" % mid)
            return

        import copy
        callbacks = copy.copy(self._callbacks[mid])
        for e in callbacks:
            cb, timeout = e
            self._callbacks[mid].remove(e)
            if timeout:
                self.io_loop.remove_timeout(timeout)
            cb(mid, prio, body)

    def _read_mid(self, data):
        self._mid, = struct.unpack('!I', data)
        self._stream.read_bytes(1, self._read_prio)


    def _read_prio(self, data):
        self._prio, = struct.unpack('B', data)
        self._stream.read_bytes(4, self._read_size)


    def _read_size(self, data):
        size, = struct.unpack('!I', data)

        if size > self._stream.max_buffer_size:
            logging.error("message size too large %d > %d (max buffer size)"
                        % (size, self._stream.max_buffer_size))

            self.close()
            raise Exception("received message size too large, close connection now")

        if size:
            self._stream.read_bytes(size, self._read_body)
            return

        self._dispatch(self._mid, self._prio)
        self.io_loop.add_callback(self._read_message)


    def _read_body(self, data):
        self._dispatch(self._mid, self._prio, data)
        self._mid = None
        self.io_loop.add_callback(self._read_message)


    def send(self, mid, prio=0, body=None):
        ''' send UL message asychronously by putting message into the sending queue
            throw an exception when sending Queue is full
        '''
        # need to check message size, unfortunately _stream is not created yet
        #if len(body) > self._stream.max_buffer_size:
        #    raise Exception("trying to send too large message")

        try:
            self._queue.put_nowait(prio,(mid,body))
        except Queue.Full:
            raise SendingQueueFull()

        if not self._sending:
            self._send()



    def _send(self):
        if not self.connected:
            return

        if self._queue.empty():
            return

        if not self._sending:
            self._sending = self._queue.get_nowait()



        prio, (mid, body) = self._sending
        size = len(body)

        def _on_send_complete():
            self._sending = None
            self._send()

        if not size:
            self._stream.write(struct.pack("!IBI", mid, prio, size), _on_send_complete)
        else:
            self._stream.write(struct.pack("!IBI", mid, prio, size))
            self._stream.write(body, _on_send_complete)



class AmppTcpMaster(object):

    def __init__(self,  port, address="", io_loop=None, ssl_options=None, max_queue_size=0):
        self._port, self._address = port, address
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._ssl_options = ssl_options
        self._socket = None
        self._started = False
        self._max_queue_size = max_queue_size

        self.queues = {}
        self.sendings = {}
        self._callbacks = {}
        self._connections = {}


    def _bind(self, port, address=""):
        assert not self._socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        flags = fcntl.fcntl(self._socket.fileno(), fcntl.F_GETFD)
        flags |= fcntl.FD_CLOEXEC
        fcntl.fcntl(self._socket.fileno(), fcntl.F_SETFD, flags)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setblocking(0)
        self._socket.bind((address, port))
        self._socket.listen(128)

    def start(self):
        self._bind(self._port, self._address)
        assert not self._started
        self._started = True
        self._io_loop.add_handler(self._socket.fileno(), self._handle_events,
                            ioloop.IOLoop.READ)

    def stop(self):
        assert self._started
        self._started = False
        self._io_loop.remove_handler(self._socket.fileno())
        self._socket.close()


    def _handle_events(self, fd, events):
        try:
            socket, address = self._socket.accept()
        except socket.error as e:
            if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return
            raise
        if self._ssl_options is not None:
            assert ssl, "Python 2.6+ and OpenSSL required for SSL"
            socket = ssl.wrap_socket(
                    socket, server_side=True, **self._ssl_options)

        try:
            stream = iostream.IOStream(socket, io_loop=self._io_loop)
            AmppTcpConnection(self, stream, address)
        except:
            logging.error("Error happened when creating a connection", exc_info=True)

    def register_connection(self, connection):
        ''' called by the connection when it registers itself properly to the master
        '''
        name = connection.name
        assert name not in self._connections.keys(), "duplicated connections " + name
        self._connections[name] = connection
        if name not in self.queues.keys():
            self.queues[name] = PrioFIFOQueue(self._max_queue_size)
        if name not in self.sendings.keys():
            self.sendings[name] = None

    def deregister_connection(self, name):
        self._connections.pop(name)


    def send(self, dest, mid, prio=0,  body=None):

        try:
            if not self._connections[dest].connected:
                logging.warning("sending messages to a broken connection " + dest )
            self.queues[dest].put_nowait(prio, (mid, body))
        except KeyError:
            logging.warning("sending messages to non-existence connection " + dest )
            self.queues[dest] = PrioFIFOQueue(self._max_queue_size)
            self.queues[dest].put_nowait(prio, (mid, body))
            self.sendings[dest] = None
        except Queue.Full :
            raise SendingQueueFull()

        try:
            conn = self._connections[dest]
            if  conn.connected and not self.sendings[dest]:
                conn.send()
        except KeyError:
            pass


    def recv(self, src, mid, callback, timeout=None):
        assert callable(callback), "callback for recv must be callable"

        try:
            if mid not in self._callbacks[src].keys():
                self._callbacks[src][mid] = []
        except KeyError:
            self._callbacks[src] = {}
            self._callbacks[src][mid] = []

        if timeout:
            def handle_timeout():
                for e in [ (f, _timeout) for (f,_timeout) in self._callbacks[src][mid]
                        if f==callback and _timeout and timeout.deadline ==_timeout.deadline]:
                    self._callbacks[src][mid].remove(e)
                    if 0 == len(self._callbacks[src][mid]):
                        self._callbacks[src].pop(mid)

                if callable(timeout.callback):
                    timeout.callback()
            _timeout = self._io_loop.add_timeout(timeout.deadline, handle_timeout)
        else:
            _timeout = None

        self._callbacks[src][mid].append((callback, _timeout))


    def dispatch(self, src, mid, prio, body=None):
        if mid not in self._callbacks[src].keys():
            logging.warning("no callbacks for mid %08X" % mid)
            return

        #callbacks = [ cb for cb in self._callbacks[src][mid] ]
        import copy
        callbacks = copy.copy(self._callbacks[src][mid])

        for e in callbacks:
            cb, timeout = e
            self._callbacks[src][mid].remove(e)
            if timeout:
                self.io_loop.remove_timeout(timeout)
            cb(src, mid, prio, body)



class AmppTcpConnection(object):

    def __init__(self, master, stream, addr):
        self._master = master
        self._stream = stream
        self.connected = False
        self._addr = addr
        self.name = None

        self._stream.read_bytes(len(MAGIC), self._verify_magic)

    def _on_stream_close(self):
        self.connected = False
        self._master.deregister_connection(self.name)

    def _verify_magic(self, data):
        if data != MAGIC:
            self._stream.close()
            logging.warning("unknown slave trying to connect from " + str(self.address))
            return
        self._stream.read_until('\0', self._read_name)


    def _read_name(self, data):
        self.name = data[:-1]
        self._stream.write(MAGIC + data)

        self.connected = True
        self._master.register_connection(self)
        self._stream.set_close_callback(self._on_stream_close)
        self._read_message()


    def _read_message(self):
        self._stream.read_bytes(4, self._read_mid)


    def _read_mid(self, data):
        self._mid, = struct.unpack('!I', data)
        self._stream.read_bytes(1, self._read_prio)


    def _read_prio(self, data):
        self._prio, = struct.unpack('B', data)
        self._stream.read_bytes(4, self._read_size)

    def _read_size(self, data):
        size,  = struct.unpack('!I', data)


        if size > self._stream.max_buffer_size:
            logging.error("message size too large %d > %d (max buffer size)"
                        % (size, self._stream.max_buffer_size))
            self.close()
            raise Exception("message size too large")

        if size:
            self._stream.read_bytes(size, self._read_body)
            return

        self.recv(self._mid, self_prio)
        self._stream.io_loop.add_callback(self._read_message)


    def _read_body(self, data):
        self.recv(self._mid, self._prio, data)
        self._mid = self._prio = None
        self._stream.io_loop.add_callback(self._read_message)

    def recv(self, mid, prio, body=None):
        self._master.dispatch(self.name, mid, prio, body)


    def send(self):
        if not self.connected:
            return

        try:
            master = self._master
            dest = self.name
            if master.sendings[dest]:
                prio, (mid, body) = master.sendings[dest]
            assert dest in master.queues.keys()
            master.sendings[dest] = prio, (mid, body) = master.queues[dest].get_nowait()
        except Queue.Empty:
            return

        size = len(body) if body else 0

        def _on_send_complete():
            self._master.sendings[dest] = None
            self.send()


        if not size:
            self._stream.write(struct.pack('!IBI', mid, prio, size), _on_send_complete)
        else:
            self._stream.write(struct.pack('!IBI', mid, prio, size))
            self._stream.write(body, _on_send_complete)


def test1():
    ampp_master = AmppTcpMaster(8000)
    ampp_master.start()
    print "master starts"

    def master_callback(src, mid, prio, body):
        #print "master receives", src, mid, prio, body
        ampp_master.send(src, mid, prio, body)
        ampp_master.recv("slave1", 100, master_callback)


    ampp_master.recv("slave1", 100, master_callback)

    def run_slave():

        ampp_slave = AmppTcpSlave("slave1", "127.0.0.1", 8000)
        ampp_slave.connect()
        print "slave starts"
        ampp_slave.send(100, body="hello world")

        def slave_callback(mid, prio, body):
            #print "slave receives",  mid, prio, body
            ampp_slave.send(mid, prio, body)
            ampp_slave.recv(100, slave_callback)

        ampp_slave.recv(100, slave_callback)

    _ioloop = ioloop.IOLoop.instance()
    run_slave()
    #_ioloop.add_timeout(time.time(),  run_slave)
    _ioloop.start()



count = 0
bytes = 0


def test2():
    from  multiprocessing import Process
    def run_master():
        print "master starts"
        ampp_master = AmppTcpMaster(8000)
        ampp_master.start()

        tic = time.time()
        def master_callback(src, mid, prio, body):
            global count, bytes
            count += 1
            bytes += len(body)
            ampp_master.send(src, mid, prio, body)
            if count < 10000:
                ampp_master.recv("slave", 100, master_callback)
            else:
                tac = time.time()
                t = tac -tic
                print t, count, bytes, bytes/t/(1024*1024)

        ampp_master.recv("slave", 100, master_callback)
        io_loop = ioloop.IOLoop.instance()
        io_loop.start()


    def run_slave():
        print "slave  starts"

        ampp_slave = AmppTcpSlave("slave", "127.0.0.1", 8000)
        ampp_slave.connect()
        ampp_slave.send(100, body="***")

        def slave_callback(mid, prio, body):
            #print "slave receives",  mid, prio, body
            ampp_slave.send(mid, prio, body)
            ampp_slave.recv(100, slave_callback)

        ampp_slave.recv(100, slave_callback)
        io_loop = ioloop.IOLoop.instance()
        io_loop.start()


    master = Process(target = run_master )
    slave = Process(target = run_slave)
    master.start()
    slave.start()

    master.join()
    slave.join()



def test3():
    from  multiprocessing import Process
    def run_master():
        print "master starts"
        ampp_master = AmppTcpMaster(8000)
        ampp_master.start()

        tic = time.time()
        def master_callback(src, mid, prio, body):
            global count, bytes
            count += 1
            bytes += len(body)
            if mid!=0:
                ampp_master.recv("slave", 100, master_callback)
            else:
                tac = time.time()
                t = tac -tic
                print t, count, count*1.0/t, bytes, bytes/t/(1024*1024)

        ampp_master.recv("slave", 100, master_callback)
        ampp_master.recv("slave", 0, master_callback)
        io_loop = ioloop.IOLoop.instance()
        io_loop.start()


    def run_slave():
        print "slave  starts"

        ampp_slave = AmppTcpSlave("slave", "127.0.0.1", 8000)
        ampp_slave.connect()

        def run_slave_send():
            import thread
            def run():
                print "start sending"
                for i in xrange(10000):
                    #ampp_slave.send(100, body='*******')
                    #ampp_slave.send(100, body='*')
                    ampp_slave.send(100, body='*'*10000 )
                ampp_slave.send(0, body='*******')
                print "sending completed"
            run()

        io_loop = ioloop.IOLoop.instance()
        io_loop.add_timeout(time.time()+0.5,  run_slave_send)
        io_loop.start()

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


#from watcher import Watcher
#Watcher()
#test2()
test3()

