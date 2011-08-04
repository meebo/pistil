# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.

import time

from pistil.arbiter import Arbiter
from pistil.worker import Worker


class MyWorker(Worker):

    def run(self):

        while True:
            print "hello from worker n°%s" % self.pid
            time.sleep(1)


if __name__ == "__main__":
    conf = {"num_workers": 3}
    a = Arbiter(conf, MyWorker)
    a.run()
