# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

from __future__ import with_statement

import os
import sys


try:
    import gevent
except ImportError:
    raise RuntimeError("You need gevent installed to use this worker.")


from gevent.pool import Pool
from gevent.server import StreamServer

from pistil import util
from pistil.tcp.sync_worker import TcpSyncWorker 

# workaround on osx, disable kqueue
if sys.platform == "darwin":
    os.environ['EVENT_NOKQUEUE'] = "1"


class PStreamServer(StreamServer):
    def __init__(self, listener, handle, spawn='default', worker=None):
        StreamServer.__init__(self, listener, spawn=spawn)
        self.handle_func = handle
        self.worker = worker

    def stop(self, timeout=None):
        super(PStreamServer, self).stop(timeout=timeout)

    def handle(self, sock, addr):
        self.handle_func(sock, addr)


class TcpGeventWorker(TcpSyncWorker):

    def on_init(self, conf):
        self.worker_connections = conf.get("worker_connections", 
                10000)
        self.pool = Pool(self.worker_connections)

    def run(self):
        self.socket.setblocking(1)
        
        # start gevent stream server
        server = PStreamServer(self.socket, self.handle, spawn=self.pool,
                worker=self)
        server.start()

        try:
            while self.alive:
                self.notify()
                if self.ppid != os.getppid():
                    self.log.info("Parent changed, shutting down: %s", self)
                    break
        
                gevent.sleep(1.0)
                
        except KeyboardInterrupt:
            pass

        try:
            # Try to stop connections until timeout
            self.notify()
            server.stop(timeout=self.timeout)
        except:
            pass


    if hasattr(gevent.core, 'dns_shutdown'):

        def init_process(self):
            #gevent 0.13 and older doesn't reinitialize dns for us after forking
            #here's the workaround
            gevent.core.dns_shutdown(fail_requests=1)
            gevent.core.dns_init()
            super(TcpGeventWorker, self).init_process()

