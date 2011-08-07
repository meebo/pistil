# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

from __future__ import with_statement

import logging
import os
import signal
import sys
import traceback


from pistil import util
from pistil.workertmp import WorkerTmp

log = logging.getLogger(__name__)

class Worker(object):
    
    _SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM USR1 USR2 WINCH CHLD".split()
    )
    
    _PIPE = []


    def __init__(self, name=None, age=0, child_type="worker",
            ppid=0, timeout=30, local_conf={}, global_conf={}):

        self._name = name
        self.child_type = child_type
        self.age = age
        self.ppid = ppid
        self.timeout = timeout

        self.local_conf = local_conf
        self.global_conf = global_conf

        # initialize
        self.booted = False
        self.alive = True
        self.debug =self.global_conf.get("debug", False)
        self.tmp = WorkerTmp(self.global_conf)

        self.on_init(self.local_conf, self.global_conf)

    def on_init(self, local_conf, global_conf):
        pass

    def __get_name(self):
        try:
            return self._name
        except AttributeError:
            self._name = self.__class__.__name__.lower()
            return self._name
    def __set_name(self, name):
        self._name = name
    name = property(__get_name, __set_name)
        
    def conf(self):
        conf = self.global_conf.copy()
        conf.update(self.local_conf)
    conf = util.cached_property(conf) 

    def pid(self):
        return os.getpid()
    pid = util.cached_property(pid)

    def notify(self):
        """\
        Your worker subclass must arrange to have this method called
        once every ``self.timeout`` seconds. If you fail in accomplishing
        this task, the master process will murder your workers.
        """
        self.tmp.notify()

    def run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()

    def on_init_process(self):
        """ method executed when we init a process """

    def init_process(self):
        """\
        If you override this method in a subclass, the last statement
        in the function should be to call this method with
        super(MyWorkerClass, self).init_process() so that the ``run()``
        loop is initiated.
        """
        util.set_owner_process(self.global_conf.get("uid", os.geteuid()),
                self.global_conf.get("gid", os.getegid()))

        # Reseed the random number generator
        util.seed()

        # For waking ourselves up
        self._PIPE = os.pipe()
        map(util.set_non_blocking, self._PIPE)
        map(util.close_on_exec, self._PIPE)
        
        # Prevent fd inherientence
        util.close_on_exec(self.tmp.fileno())
        self.init_signals()
        
        self.on_init_process()

        # Enter main run loop
        self.booted = True
        self.run()

    def init_signals(self):
        map(lambda s: signal.signal(s, signal.SIG_DFL), self._SIGNALS)
        signal.signal(signal.SIGQUIT, self.handle_quit)
        signal.signal(signal.SIGTERM, self.handle_exit)
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGWINCH, self.handle_winch)
            
    def handle_quit(self, sig, frame):
        self.alive = False

    def handle_exit(self, sig, frame):
        self.alive = False
        sys.exit(0)
        
    def handle_winch(self, sig, fname):
        # Ignore SIGWINCH in worker. Fixes a crash on OpenBSD.
        return
