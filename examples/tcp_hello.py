# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

from pistil.tcp.sync_worker import TcpSyncWorker
from pistil.tcp.arbiter import TcpArbiter

from http_parser.http import HttpStream
from http_parser.reader import SocketReader

class MyTcpWorker(TcpSyncWorker):

    def handle(self, sock, addr):
        p = HttpStream(SocketReader(sock))

        path = p.path()
        data = "hello world"
        sock.send("".join(["HTTP/1.1 200 OK\r\n", 
                        "Content-Type: text/html\r\n",
                        "Content-Length:" + str(len(data)) + "\r\n",
                         "Connection: close\r\n\r\n",
                         data]))

if __name__ == '__main__':
    conf = {"num_workers": 3}
    spec = (MyTcpWorker, 30, "worker", {}, "worker",)
    
    arbiter = TcpArbiter(conf, spec)

    arbiter.run()

