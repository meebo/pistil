# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license. 
# See the NOTICE for more information.

from __future__ import with_statement

import errno
import logging
from logging.config import fileConfig
import os
import select
import signal
import sys
import time
import traceback

from pistil.errors import HaltServer
from pistil.pidfile import Pidfile
from pistil.workertmp import WorkerTmp
from pistil import util
from pistil import __version__, SERVER_SOFTWARE

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG
}

DEFAULT_CONF = dict(
    name = __name__,
    pidfile = None,
    worker_connections = 1000,
    num_workers = 1, 
    max_requests = 0,
    timeout = 30,
    uid = os.geteuid(),
    gid = os.getegid(),
    umask = 0,
    debug = False,
    log_config = None,
    log_file = '-',
    log_level= 'info')


RESTART__WORKERS = ("worker", "supervisor")

def configure_logging(loglevel='info', logfile='-', logconfig=None):
    """\
    Set the log level and choose the destination for log output.
    """
    logger = logging.getLogger(__name__)

    fmt = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
    datefmt = r"%Y-%m-%d %H:%M:%S"
    if not conf.get('log_config'):
        handlers = []
        if logfile != "-":
            handlers.append(logging.FileHandler(logfile))
        else:
            handlers.append(logging.StreamHandler())

        loglevel = LOG_LEVELS[loglevel] 
        logger.setLevel(loglevel)
        for h in handlers:
            h.setFormatter(logging.Formatter(fmt, datefmt))
            logger.addHandler(h)
    else:
        if os.path.exists(logconfig):
            fileConfig(logconfig)
        else:
            raise RuntimeError("Error: logfile '%s' not found." %
                    logconfig)


log = logging.getLogger(__name__)


logging.basicConfig(format="%(asctime)s [%(process)d] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)



class child(object):


    def __init__(self, child_class, timeout=30, child_type="worker",
            args={}, name=None):
        self.child_class= child_class
        self.timeout = timeout
        self.child_type = child_type
        self.args = args
        self.name = name
   

    def __get__(self, instance, cls):
        if instance is None:
            return self
        return instance._CHILDREN_SPECS[self.name]


    def __child_config__(self, cls, name):
        if self.name is None:
            self.name = name

    def __set__(self, instance, value):
        instance._CHILDREN_SPECS[self.name] = value

    def __property_init__(self, document_instance, value):
        """ method used to set value of the property when
        we create the document. Don't check required. """
        if value is not None:
            value = self.to_json(self.validate(value, required=False))
        document_instance._doc[self.name] = value


class MetaArbiter(type):

    def __new__(cls, name, bases, attrs):
        # init properties
        children = {}
        defined = set()
        for base in bases:
            if hasattr(base, '_CHILDREN_SPECS'):
                child_names  = base._CHILDREN_SPECS.keys()
                duplicate_names = defined.intersection(child_names)
                if duplicate_names:
                    raise DuplicatePropertyError(
                            'Duplicate children in base class %s already defined: %s' % (base.__name__, list(duplicate_names)))
                    defined.update(duplicate_names)
                children.update(base._CHILDREN_SPECS)



        for attr_name, attr in attrs.items():
            # map properties
            if isinstance(attr, child):
                print attr_name
                if attr_name in defined:
                    raise DuplicatePropertyError('Duplicate child: %s' % attr_name)
                children[attr_name] = attr
                attr.__child_config__(cls, attr_name)
        attrs['_CHILDREN_SPECS'] = children
        return type.__new__(cls, name, bases, attrs)


# chaine init worker:
# (WorkerClass, max_requests, timeout, type, args, name)
# types: supervisor, kill, brutal_kill, worker

class Arbiter(object):
    """
    Arbiter maintain the workers processes alive. It launches or
    kills them if needed. It also manages application reloading
    via SIGHUP/USR2.
    """

    __metaclass__ = MetaArbiter 

    _CHILDREN_SPECS = dict()

    # A flag indicating if a worker failed to
    # to boot. If a worker process exist with
    # this error code, the arbiter will terminate.
    _WORKER_BOOT_ERROR = 3

    _WORKERS = {}    
    _PIPE = []

    # I love dynamic languages
    _SIG_QUEUE = []
    _SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM USR1 WINCH".split()
    )
    _SIG_NAMES = dict(
        (getattr(signal, name), name[3:].lower()) for name in dir(signal)
        if name[:3] == "SIG" and name[3] != "_"
    )
    
    def __init__(self, name=None, child_type="supervisor", age=0,
            ppid=0, timeout=30, local_conf={}, global_conf={}):

        self._name = name
        self.child_type = child_type
        self.age = age
        self.ppid = ppid
        self.timeout = timeout
        self.local_conf = local_conf 
        self.global_conf = global_conf

        self.pid = None
        self.num_children = len(self._CHILDREN_SPECS.keys())
        self.child_age = 0
        self.booted = False
        self.stopping = False
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
    name = util.cached_property(__get_name, __set_name)

    def on_init_process(self):
        """ method executed when we init a process """

    def init_process(self):
        """\
        If you override this method in a subclass, the last statement
        in the function should be to call this method with
        super(MyWorkerClass, self).init_process() so that the ``run()``
        loop is initiated.
        """

        # set current pid
        self.pid = os.getpid()

        util.set_owner_process(self.global_conf.get("uid", os.geteuid()),
                self.global_conf.get("gid", os.getegid()))

        # Reseed the random number generator
        util.seed()

        # init signals
        self.init_signals()

        self.on_init_process()

        log.debug("Arbiter %s booted on %s", self.name, self.pid)
        self.when_ready()
        # Enter main run loop
        self.booted = True
        self.run()
    

    def when_ready(self):
        pass

    def init_signals(self):
        """\
        Initialize master signal handling. Most of the signals
        are queued. Child signals only wake up the master.
        """
        if self._PIPE:
            map(os.close, self._PIPE)
        self._PIPE = pair = os.pipe()
        map(util.set_non_blocking, pair)
        map(util.close_on_exec, pair)
        map(lambda s: signal.signal(s, self.signal), self._SIGNALS)
        signal.signal(signal.SIGCHLD, self.handle_chld)

    def signal(self, sig, frame):
        if len(self._SIG_QUEUE) < 5:
            self._SIG_QUEUE.append(sig)
            self.wakeup()
        else:
            log.warn("Dropping signal: %s", sig)

    def run(self):
        "Main master loop."
        util._setproctitle("arbiter [%s]" % self.name)
       
        if not self.booted:
            return self.init_process()

        self.spawn_workers()
        while True:
            try:
                self.reap_workers()
                sig = self._SIG_QUEUE.pop(0) if len(self._SIG_QUEUE) else None
                if sig is None:
                    self.sleep()
                    self.murder_workers()
                    self.manage_workers()
                    continue
                
                if sig not in self._SIG_NAMES:
                    log.info("Ignoring unknown signal: %s", sig)
                    continue
                
                signame = self._SIG_NAMES.get(sig)
                handler = getattr(self, "handle_%s" % signame, None)
                if not handler:
                    log.error("Unhandled signal: %s", signame)
                    continue
                log.info("Handling signal: %s", signame)
                handler()  
                self.wakeup()
            except StopIteration:
                self.halt()
            except KeyboardInterrupt:
                self.halt()
            except HaltServer, inst:
                self.halt(reason=inst.reason, exit_status=inst.exit_status)
            except SystemExit:
                raise
            except Exception:
                log.info("Unhandled exception in main loop:\n%s",  
                            traceback.format_exc())
                self.stop(False)
                sys.exit(-1)

    def handle_chld(self, sig, frame):
        "SIGCHLD handling"
        self.wakeup()
        self.reap_workers()
        
    def handle_hup(self):
        """\
        HUP handling.
        - Reload configuration
        - Start the new worker processes with a new configuration
        - Gracefully shutdown the old worker processes
        """
        log.info("Hang up: %s", self.name)
        self.reload()
        
    def handle_quit(self):
        "SIGQUIT handling"
        raise StopIteration
    
    def handle_int(self):
        "SIGINT handling"
        raise StopIteration
    
    def handle_term(self):
        "SIGTERM handling"
        self.stop(False)
        raise StopIteration


    def handle_usr1(self):
        """\
        SIGUSR1 handling.
        Kill all workers by sending them a SIGUSR1
        """
        self.kill_workers(signal.SIGUSR1)
    
    def handle_winch(self):
        "SIGWINCH handling"
        if os.getppid() == 1 or os.getpgrp() != os.getpid():
            log.info("graceful stop of workers")
            self.num_workers = 0
            self.kill_workers(signal.SIGQUIT)
        else:
            log.info("SIGWINCH ignored. Not daemonized")
    
    def wakeup(self):
        """\
        Wake up the arbiter by writing to the _PIPE
        """
        try:
            os.write(self._PIPE[1], '.')
        except IOError, e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise
                    
    def halt(self, reason=None, exit_status=0):
        """ halt arbiter """
        log.info("Shutting down: %s", self.name)
        if reason is not None:
            log.info("Reason: %s", reason)
        self.stop()
        log.info("See you next")
        sys.exit(exit_status)
        
    def sleep(self):
        """\
        Sleep until _PIPE is readable or we timeout.
        A readable _PIPE means a signal occurred.
        """
        try:
            ready = select.select([self._PIPE[0]], [], [], 1.0)
            if not ready[0]:
                return
            while os.read(self._PIPE[0], 1):
                pass
        except select.error, e:
            if e[0] not in [errno.EAGAIN, errno.EINTR]:
                raise
        except OSError, e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise
        except KeyboardInterrupt:
            sys.exit()
            
    
    def on_stop(self, graceful=True):
        """ method used to pass code when the server start """

    def stop(self, graceful=True):
        """\
        Stop workers
        
        :attr graceful: boolean, If True (the default) workers will be
        killed gracefully  (ie. trying to wait for the current connection)
        """
        
        ## pass any actions before we effectively stop
        self.on_stop(graceful=graceful)
        self.stopping = True
        sig = signal.SIGQUIT
        if not graceful:
            sig = signal.SIGTERM
        limit = time.time() + self.timeout
        while True:
            if time.time() >= limit or not self._WORKERS:
                break
            self.kill_workers(sig)
            time.sleep(0.1)
            self.reap_workers()
        self.kill_workers(signal.SIGKILL)   
        self.stopping = False

    def on_reload(self, conf, old_conf):
        """ method executed on reload """


    def reload(self):
        """ 
        used on HUP
        """
    
        # keep oldconf
        old_global_conf = self.global_conf.copy()
        old_local_conf = self.local_conf.copy()
        
        # exec on reload hook
        self.on_reload(self.local_conf, old_local_conf,
                self.global_conf, self.old_global_conf)

        OLD__WORKERS = self._WORKERS.copy()

        # don't kill
        to_reload = []

        # spawn new workers with new app & conf
        for child in self._CHILDREN_SPECS:
            if child.child_type != "supervisor":
                to_reload.append(child)

        # set new proc_name
        util._setproctitle("arbiter [%s]" % self.name)
        
        # kill old workers
        for wpid, (child, state) in OLD__WORKERS.items():
            if state:
                if child.child_type == "supervisor":
                    # we only reload suprvisors.
                    sig = signal.SIGHUP
                elif child.child_type == "brutal_kill":
                    sig =  signal.SIGTERM
                else:
                    sig =  signal.SIGQUIT
                self.kill_worker(wpid, sig)

        
    def murder_workers(self):
        """\
        Kill unused/idle workers
        """
        for (pid, child_info) in self._WORKERS.items():
            (child, state) = child_info
            if state:
                try:
                    diff = time.time() - os.fstat(child.tmp.fileno()).st_ctime
                    if diff <= child.timeout:
                        continue
                except ValueError:
                    continue

            log.critical("WORKER TIMEOUT (pid:%s)", pid)
            self.kill_worker(pid, signal.SIGKILL)
        
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
                if child.child_type in RESTART__WORKERS and not self.stopping:
                    self._WORKERS["<killed %s>"  % id(child)] = (child, 0)
        except OSError, e:
            if e.errno == errno.ECHILD:
                pass
    
    def manage_workers(self):
        """\
        Maintain the number of workers by spawning or killing
        as required.
        """

        for pid, (child, state) in self._WORKERS.items():
            if not state:
                self.spawn_child(self._CHILDREN_SPECS[child.name])

    def pre_fork(self, worker):
        """ methode executed on prefork """

    def post_fork(self, worker):
        """ method executed after we forked a worker """
            
    def spawn_child(self, child_spec):
        self.child_age += 1
        name = child_spec.name
        child_type = child_spec.child_type

        child = child_spec.child_class(name, 
                    age=self.child_age,
                    child_type=child_type, 
                    ppid = self.pid,
                    timeout=child_spec.timeout, 
                    local_conf=child_spec.args,
                    global_conf=self.global_conf)

        self.pre_fork(child)
        pid = os.fork()
        if pid != 0:
            self._WORKERS[pid] = (child, 1)
            return

        # Process Child
        worker_pid = os.getpid()
        try:
            util._setproctitle("worker %s [%s]" % (name,  worker_pid))
            log.info("Booting %s (%s) with pid: %s", name,
                    child_type, worker_pid)
            self.post_fork(child)
            child.init_process()
            sys.exit(0)
        except SystemExit:
            raise
        except:
            log.exception("Exception in worker process:")
            if not child.booted:
                sys.exit(self._WORKER_BOOT_ERROR)
            sys.exit(-1)
        finally:
            log.info("Worker exiting (pid: %s)", worker_pid)
            try:
                child.tmp.close()
            except:
                pass

    def spawn_workers(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        
        for child_name, child in self._CHILDREN_SPECS.items(): 
            self.spawn_child(child)

    def kill_workers(self, sig):
        """\
        Lill all workers with the signal `sig`
        :attr sig: `signal.SIG*` value
        """
        for pid in self._WORKERS.keys():
            self.kill_worker(pid, sig)
                   

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
           
                    if not self.stopping:
                        self._WORKERS["<killed %s>"  % id(child)] = (child, 0)
                    return
                except (KeyError, OSError):
                    return
            raise            
