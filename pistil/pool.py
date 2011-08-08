# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

import errno
import os
import signal

from pistil.errors import HaltServer
from pistil.arbiter import Arbiter, Child
from pistil.workertmp import WorkerTmp
from pistil import util

DEFAULT_CONF = dict(
    uid = os.geteuid(),
    gid = os.getegid(),
    umask = 0,
    debug = False,
    num_workers = 1,
)


class PoolArbiter(Arbiter):


    _SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM TTIN TTOU USR1 USR2 WINCH".split()
    )

    def __init__(self, args, spec=(), name=None,
            child_type="supervisor", age=0, ppid=0,
            timeout=30):

        if not isinstance(spec, tuple):
            raise TypeError("spec should be a tuple")

        # set conf
        conf = DEFAULT_CONF.copy()
        conf.update(args)
        self.conf = conf

        # set number of workers
        self.num_workers = conf.get('num_workers', 1)
       
        ret =  self.on_init(conf)
        if not ret: 
            self._SPEC = Child(*spec)
        else:
            self._SPEC = Child(*ret)
        
        if name is None:
            name =  self.__class__.__name__

        self.name = name
        self.child_type = child_type
        self.age = age
        self.ppid = ppid
        self.timeout = timeout


        self.pid = None
        self.child_age = 0
        self.booted = False
        self.stopping = False
        self.debug =self.conf.get("debug", False)
        self.tmp = WorkerTmp(self.conf) 

    def update_proc_title(self):
        util._setproctitle("arbiter [%s running %s workers]" % (self.name,  
            self.num_workers))

    def on_init(self, conf):
        return None

    def on_init_process(self):
        self.update_proc_title()
        
    def handle_ttin(self):
        """\
        SIGTTIN handling.
        Increases the number of workers by one.
        """
        self.num_workers += 1
        self.update_proc_title()
        self.manage_workers()
    
    def handle_ttou(self):
        """\
        SIGTTOU handling.
        Decreases the number of workers by one.
        """
        if self.num_workers <= 1:
            return
        self.num_workers -= 1
        self.update_proc_title()
        self.manage_workers()

    def reload(self):
        """ 
        used on HUP
        """

        # exec on reload hook
        self.on_reload()

        # spawn new workers with new app & conf
        for i in range(self.conf.get("num_workers", 1)):
            self.spawn_child(self._SPEC)
            
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
            self.spawn_workers()

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
        for i in range(self.num_workers - len(self._WORKERS.keys())):
            self.spawn_child(self._SPEC)
            
    
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
