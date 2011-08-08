# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

from pistil.arbiter import child
from pistil.tcp.sync_worker import TcpSyncWorker
from pistil.tcp.arbiter import TcpArbiter

from http_parser.http import HttpStream
from http_parser.reader import SocketReader

class MyTcpWorker(TcpSyncWorker):

    def handle(self, sock, addr):
        p = HttpStream(SocketReader(sock))

        path = p.path()
        data = "welcome wold 2"
        sock.send("".join(["HTTP/1.1 200 OK\r\n", 
                        "Content-Type: text/html\r\n",
                        "Content-Length:" + str(len(data)) + "\r\n",
                         "Connection: close\r\n\r\n",
                         data]))


class MyPoolArbiter(TcpArbiter):
    worker = child(MyTcpWorker, 30, "worker", {})


if __name__ == '__main__':
    arbiter = MyPoolArbiter("master", local_conf={"num_workers": 3})

    arbiter.run()

