# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

from pistil.pool import PoolArbiter
from pistil.worker import Worker

class MyWorker(Worker):

    def handle(self):
        print "hello from worker n°%s" % self.pid

if __name__ == "__main__":
    conf = {"num_workers": 3 }
    spec = (MyWorker, 30, "worker", {}, "test",)
    a = PoolArbiter(conf, spec)
    a.run()
