# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import errno
import os
import signal

from pistil.arbiter import Arbiter
from pistil import util

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

    def reload(self):
        """ 
        used on HUP
        """
    
        # keep oldconf
        old_global_conf = self.global_conf.copy()
        old_local_conf = self.local_conf.copy()
        
        # exec on reload hook
        self.on_reload(self.local_conf, old_local_conf,
                self.global_conf, old_global_conf)


        # spawn new workers with new app & conf
        child = self._CHILDREN_SPECS["worker"]
        for i in range(self.local_conf.get("num_workers", 1)):
            self.spawn_child(child)
            
        # set new proc_name
        util._setproctitle("master [%s]" % self.name)
        
        # manage workers
        self.manage_workers() 


    def reap_workers(self):
        """\
        Reap workers to avoid zombie processes
        """
        try:
            while True:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid:
                    break
                
                # A worker said it cannot boot. We'll shutdown
                # to avoid infinite start/stop cycles.
                exitcode = status >> 8
                if exitcode == self._WORKER_BOOT_ERROR:
                    reason = "Worker failed to boot."
                    raise HaltServer(reason, self._WORKER_BOOT_ERROR)
                child_info = self._WORKERS.pop(wpid, None)

                if not child_info:
                    continue

                child, state = child_info
                child.tmp.close()
        except OSError, e:
            if e.errno == errno.ECHILD:
                pass

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
            
    
    def kill_worker(self, pid, sig):
        """\
               Kill a worker

        :attr pid: int, worker pid
        :attr sig: `signal.SIG*` value
         """
        if not isinstance(pid, int):
            return

        try:
            os.kill(pid, sig)
        except OSError, e:
            if e.errno == errno.ESRCH:
                try:
                    (child, info) = self._WORKERS.pop(pid)
                    child.tmp.close()
                    return
                except (KeyError, OSError):
                    return
            raise  
