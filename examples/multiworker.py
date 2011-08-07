# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import time
from pistil.arbiter import Arbiter, child
from pistil.worker import Worker
from http_parser.http import HttpStream
from http_parser.reader import SocketReader

class MyWorker(Worker):

    def run(self):
        while self.alive:
            time.sleep(0.1)
            print "hello %s" % self.name
            self.notify()

class MyWorker2(Worker):

    def run(self):
        print "yo"
        while self.alive:
            time.sleep(0.1)
            print "hello 2 %s" % self.name
            self.notify()


class MyArbiter(Arbiter):
    worker = child(MyWorker, 30, "worker", {})
    worker2 = child(MyWorker2, 30, "worker", {})
    worker3 = child(MyWorker2, 30, "kill", {})


if __name__ == '__main__':
    arbiter = MyArbiter("master")

    arbiter.run()
