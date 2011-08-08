# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import time
import urllib2

from pistil.arbiter import Arbiter
from pistil.worker import Worker
from pistil.tcp.sync_worker import TcpSyncWorker
from pistil.tcp.arbiter import TcpArbiter

from http_parser.http import HttpStream
from http_parser.reader import SocketReader

class MyTcpWorker(TcpSyncWorker):

    def handle(self, sock, addr):
        p = HttpStream(SocketReader(sock))

        path = p.path()
        data = "welcome wold"
        sock.send("".join(["HTTP/1.1 200 OK\r\n", 
                        "Content-Type: text/html\r\n",
                        "Content-Length:" + str(len(data)) + "\r\n",
                         "Connection: close\r\n\r\n",
                         data]))


class UrlWorker(Worker):

    def run(self):
        print "ici"
        while self.alive: 
            time.sleep(0.1)
            f = urllib2.urlopen("http://localhost:5000")
            print f.read()
            self.notify() 

class MyPoolArbiter(TcpArbiter):

    def on_init(self, conf):
        TcpArbiter.on_init(self, conf)
        # we return a spec
        return (MyTcpWorker, 30, "worker", {}, "http_welcome",)


if __name__ == '__main__':
    conf = {"num_workers": 3, "address": ("127.0.0.1", 5000)}

    specs = [
        (MyPoolArbiter, 30, "supervisor", {}, "tcp_pool"),
        (UrlWorker, 30, "worker", {}, "grabber")
    ]

    arbiter = Arbiter(conf, specs)
    arbiter.run()




