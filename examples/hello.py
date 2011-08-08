# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import time

from pistil.arbiter import Arbiter
from pistil.worker import Worker


class MyWorker(Worker):

    def handle(self):
        print "hello from worker n°%s" % self.pid


if __name__ == "__main__":
    conf = {}
    specs = [(MyWorker, 30, "worker", {}, "test")]
    a = Arbiter(conf, specs)
    a.run()
