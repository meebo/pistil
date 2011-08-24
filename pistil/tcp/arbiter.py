# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import logging
import os

from pistil import util
from pistil.pool import PoolArbiter
from pistil.tcp.sock import create_socket

log = logging.getLogger(__name__)


class TcpArbiter(PoolArbiter):

    _LISTENER = None

    def on_init(self, args):
        self.address = util.parse_address(args.get('address',
            ('127.0.0.1', 8000)))
        if not self._LISTENER:
            self._LISTENER = create_socket(args)

        # we want to pass the socket to the worker.
        self.conf.update({"sock": self._LISTENER})
        

    def when_ready(self):
        log.info("Listening at: %s (%s)", self._LISTENER,
            self.pid)
   
    def on_reexec(self):
        # save the socket file descriptor number in environ to reuse the
        # socket after forking a new master.
        os.environ['PISTIL_FD'] = str(self._LISTENER.fileno())

    def on_stop(self, graceful=True):
        self._LISTENER = None



