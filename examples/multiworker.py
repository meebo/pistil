# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

from pistil.arbiter import Arbiter
from pistil.worker import Worker


class MyWorker(Worker):

    def handle(self): 
        print "hello worker 1 from %s" % self.name

class MyWorker2(Worker):

    def handle(self):
        print "hello worker 2 from %s" % self.name


if __name__ == '__main__':
    conf = {}

    specs = [
        (MyWorker, 30, "worker", {}, "w1"),
        (MyWorker2, 30, "worker", {}, "w2"),
        (MyWorker2, 30, "kill", {}, "w3")
    ]
    # launchh the arbiter
    arbiter = Arbiter(conf, specs)
    arbiter.run()
