# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import logging
import os

from pistil.pool import PoolArbiter
from pistil.tcp.sock import create_socket

log = logging.getLogger(__name__)


class TcpArbiter(PoolArbiter):

    _LISTENER = None

    def on_init(self, local_conf, global_conf):
        super(TcpArbiter, self).on_init(local_conf, global_conf)
        self.address = local_conf.get('address', ('127.0.0.1', 8000))
        if not self._LISTENER:
            self._LISTENER = create_socket(self.local_conf)

        # we want to pass the socket to the worker.
        self.worker.args = {"sock": self._LISTENER}
        

    def when_ready(self):
        log.info("Listening at: %s (%s)", self._LISTENER,
            self.pid)
   
    def on_reexec(self):
        # save the socket file descriptor number in environ to reuse the
        # socket after forking a new master.
        os.environ['PISTIL_FD'] = str(self._LISTENER.fileno())

    def on_reload(self, local_conf, old_local_conf, global_conf,
            old_global_conf):
        old_address = old_conf.get("address", ('127.0.0.1', 8000))

        # do we need to change listener ?
        if old_address != conf.get("address", ('127.0.0.1', 8000)):
            self._LISTENER.close()
            self._LISTENER = create_socket(conf)
            log.info("Listening at: %s", self._LISTENER) 

    def on_stop(self, graceful=True):
        self._LISTENER = None



