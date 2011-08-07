# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import signal

from pistil.arbiter import Arbiter


class MetaPoolArbiter(type):

    def __new__(cls, name, bases, attrs):
        if "worker" in attrs:
            attrs['_CHILDREN_SPECS']['worker'] = attrs.get('worker')
        else:
            raise AttributeError("A worker is required")
        return type.__new__(cls, name, bases, attrs)


class PoolArbiter(Arbiter):


    SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM TTIN TTOU USR1 USR2 WINCH".split()
    )

    def __init__(self, name=None, child_type="supervisor", age=0,
            ppid=0, timeout=30, local_conf={}, global_conf={}):
        self.num_workers = local_conf.get('num_workers', 1)
        Arbiter.__init__(self, name=name, child_type=child_type, 
                age=age, ppid=ppid, timeout=timeout, local_conf=local_conf,
                global_conf=global_conf)


    def handle_ttin(self):
        """\
        SIGTTIN handling.
        Increases the number of workers by one.
        """
        self.num_workers += 1
        self.manage_workers()
    
    def handle_ttou(self):
        """\
        SIGTTOU handling.
        Decreases the number of workers by one.
        """
        if self.num_workers <= 1:
            return
        self.num_workers -= 1
        self.manage_workers()

    def manage_workers(self):
        """\
        Maintain the number of workers by spawning or killing
        as required.
        """
        if len(self._WORKERS.keys()) < self.num_workers:
            self.spawn_children()

        workers = self._WORKERS.items()
        workers.sort(key=lambda w: w[1][0].age)
        while len(workers) > self.num_workers:
            (pid, _) = workers.pop(0)
            self.kill_worker(pid, signal.SIGQUIT)

    def spawn_workers(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        child = self._CHILDREN_SPECS.get('worker')
        for i in range(self.num_workers - len(self._WORKERS.keys())):
            self.spawn_child(child)
    
