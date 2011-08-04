# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.


import os

from pistil.arbiter import Arbiter

from pistil.tcp.sock import create_socket

class TcpArbiter(Arbiter):

    LISTENER = None

    def on_setup(self):
        # set address
        self.address = self.conf.get('address', ('127.0.0.1', 8000))

    def on_starting(self):
        if not self.LISTENER:
            self.LISTENER = create_socket(self.conf)

        # we want to pass the socket to the worker.
        self.worker_args = {"sock": self.LISTENER}

    def when_ready(self):
        self.log.info("Listening at: %s (%s)", self.LISTENER,
            self.pid)
   
    def on_reexec(self):
        # save the socket file descriptor number in environ to reuse the
        # socket after forking a new master.
        os.environ['PISTIL_FD'] = str(self.LISTENER.fileno())

    def on_reload(self, conf, old_conf):
        old_address = old_conf.get("address", ('127.0.0.1', 8000))

        # do we need to change listener ?
        if old_address != conf.get("address", ('127.0.0.1', 8000)):
            self.LISTENER.close()
            self.LISTENER = create_socket(conf)
            self.log.info("Listening at: %s", self.LISTENER) 

    def on_stop(self, graceful=True):
        self.LISTENER = None



