# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import errno
import logging
import os
import select
import socket


from pistil import util
from pistil.worker import Worker


log = logging.getLogger(__name__)


class TcpSyncWorker(Worker):

    def on_init_process(self):
        self.socket = self.conf.get('sock')
        self.address = self.socket.getsockname()
        util.close_on_exec(self.socket)
        
    def run(self):
        self.socket.setblocking(0)

        while self.alive:
            self.notify()

            # Accept a connection. If we get an error telling us
            # that no connection is waiting we fall down to the
            # select which is where we'll wait for a bit for new
            # workers to come give us some love.
            try:
                client, addr = self.socket.accept()
                client.setblocking(1)
                util.close_on_exec(client)
                self.handle(client, addr)

                # Keep processing clients until no one is waiting. This
                # prevents the need to select() for every client that we
                # process.
                continue

            except socket.error, e:
                if e[0] not in (errno.EAGAIN, errno.ECONNABORTED):
                    raise

            # If our parent changed then we shut down.
            if self.ppid != os.getppid():
                log.info("Parent changed, shutting down: %s", self)
                return
            
            try:
                self.notify()
                ret = select.select([self.socket], [], self._PIPE,
                        self.timeout / 2.0)
                if ret[0]:
                    continue
            except select.error, e:
                if e[0] == errno.EINTR:
                    continue
                if e[0] == errno.EBADF:
                    if self.nr < 0:
                        continue
                    else:
                        return
                raise

    def handle(self, client, addr):
        raise NotImplementedError
