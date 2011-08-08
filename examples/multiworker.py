# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import time
from pistil.arbiter import Arbiter
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


if __name__ == '__main__':

    conf = {}

    specs = [
        (MyWorker, 30, "worker", {}, "w1"),
        (MyWorker2, 30, "worker", {}, "w2"),
        (MyWorker2, 30, "kill", {}, "w3")
    ]


    arbiter = Arbiter(conf, specs)

    arbiter.run()
