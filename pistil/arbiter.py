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



def configure_logging(conf):
    """\
    Set the log level and choose the destination for log output.
    """
    logger = logging.getLogger(__name__)

    fmt = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
    datefmt = r"%Y-%m-%d %H:%M:%S"
    if not conf.get('log_config'):
        handlers = []
        logfile = conf.get('log_file', '-') 
        if logfile != "-":
            handlers.append(logging.FileHandler(logfile))
        else:
            handlers.append(logging.StreamHandler())

        loglevel = LOG_LEVELS.get(conf.get('loglevel', 'info').lower(), 
                logging.INFO)
        logger.setLevel(loglevel)
        for h in handlers:
            h.setFormatter(logging.Formatter(fmt, datefmt))
            logger.addHandler(h)
    else:
        logconfig = conf.get('log_config')
        if os.path.exists(logconfig):
            fileConfig(logconfig)
        else:
            raise RuntimeError("Error: logfile '%s' not found." %
                    logconfig)


class Arbiter(object):
    """
    Arbiter maintain the workers processes alive. It launches or
    kills them if needed. It also manages application reloading
    via SIGHUP/USR2.
    """

    # A flag indicating if a worker failed to
    # to boot. If a worker process exist with
    # this error code, the arbiter will terminate.
    WORKER_BOOT_ERROR = 3

    START_CTX = {}
    
    WORKERS = {}    
    PIPE = []

    # I love dynamic languages
    SIG_QUEUE = []
    SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM TTIN TTOU USR1 USR2 WINCH".split()
    )
    SIG_NAMES = dict(
        (getattr(signal, name), name[3:].lower()) for name in dir(signal)
        if name[:3] == "SIG" and name[3] != "_"
    )
    
    def __init__(self, conf, worker_class, worker_args=None,
            set_logging=configure_logging):

        if configure_logging is not None:
            configure_logging(conf)

        # initialize conf
        self.conf = DEFAULT_CONF.copy()
        self.conf.update(conf)

        # set worker class
        self.worker_class = worker_class
        if hasattr(self.worker_class, "setup"):
            self.worker_class.setup(self, conf)
        self.worker_args = worker_args

        self.log = logging.getLogger(__name__)
       
        os.environ["SERVER_SOFTWARE"] = SERVER_SOFTWARE

        self.setup()
        
        self.pidfile = None
        self.worker_age = 0
        self.reexec_pid = 0
        self.master_name = "Master"
        
        # get current path, try to use PWD env first
        try:
            a = os.stat(os.environ['PWD'])
            b = os.stat(os.getcwd())
            if a.ino == b.ino and a.dev == b.dev:
                cwd = os.environ['PWD']
            else:
                cwd = os.getcwd()
        except:
            cwd = os.getcwd()
            
        args = sys.argv[:]
        args.insert(0, sys.executable)

        # init start context
        self.START_CTX = {
            "args": args,
            "cwd": cwd,
            0: sys.executable
        }
       

    def on_setup(self):
        """ method executed when we setup the arbiter """

    def setup(self):
        self.num_workers = self.conf.get("num_workers", 1) 
        self.debug = self.conf.get("debug", False)
        self.timeout = self.conf.get("timeout", 30)
        self.proc_name = self.conf.get("name", __name__)
       
        if self.debug:
            self.log.debug("Current configuration:")
            for config, value in sorted(self.conf.items()):
                self.log.debug("  %s: %s", config, value)

        self.on_setup()

       

    def on_starting(self):
        """ hook executed when the arbiter start """
        pass

    def when_ready(self):
        """ hooks to exectute when the arbiter is ready:
        - signals are set, 
        - pidfile set
        """
        pass


    def start(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        
        # exec hook
        self.on_starting()

        # iget current pid
        self.pid = os.getpid()

        # init signals
        self.init_signals()
        
        # set pidfile
        if self.conf.get('pidfile') is not None:
            self.pidfile = Pidfile(self.conf.get("pidfile"))
            self.pidfile.create(self.pid)

        self.log.info("Starting %s %s", self.conf.get("name", __name__), __version__)
        self.log.debug("Arbiter booted")
        self.log.info("Using worker: %s", self.worker_class)

        self.when_ready()
    
    def init_signals(self):
        """\
        Initialize master signal handling. Most of the signals
        are queued. Child signals only wake up the master.
        """
        if self.PIPE:
            map(os.close, self.PIPE)
        self.PIPE = pair = os.pipe()
        map(util.set_non_blocking, pair)
        map(util.close_on_exec, pair)
        map(lambda s: signal.signal(s, self.signal), self.SIGNALS)
        signal.signal(signal.SIGCHLD, self.handle_chld)

    def signal(self, sig, frame):
        if len(self.SIG_QUEUE) < 5:
            self.SIG_QUEUE.append(sig)
            self.wakeup()
        else:
            self.log.warn("Dropping signal: %s", sig)

    def run(self):
        "Main master loop."
        self.start()
        util._setproctitle("master [%s]" % self.proc_name)
        
        self.manage_workers()
        while True:
            try:
                self.reap_workers()
                sig = self.SIG_QUEUE.pop(0) if len(self.SIG_QUEUE) else None
                if sig is None:
                    self.sleep()
                    self.murder_workers()
                    self.manage_workers()
                    continue
                
                if sig not in self.SIG_NAMES:
                    self.log.info("Ignoring unknown signal: %s", sig)
                    continue
                
                signame = self.SIG_NAMES.get(sig)
                handler = getattr(self, "handle_%s" % signame, None)
                if not handler:
                    self.log.error("Unhandled signal: %s", signame)
                    continue
                self.log.info("Handling signal: %s", signame)
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
                self.log.info("Unhandled exception in main loop:\n%s",  
                            traceback.format_exc())
                self.stop(False)
                if self.pidfile is not None:
                    self.pidfile.unlink()
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
        self.log.info("Hang up: %s", self.master_name)
        self.reload()
        
    def handle_quit(self):
        "SIGQUIT handling"
        raise StopIteration
    
    def handle_int(self):
        "SIGINT handling"
        self.stop(False)
        raise StopIteration
    
    def handle_term(self):
        "SIGTERM handling"
        self.stop(False)
        raise StopIteration

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

    def handle_usr1(self):
        """\
        SIGUSR1 handling.
        Kill all workers by sending them a SIGUSR1
        """
        self.kill_workers(signal.SIGUSR1)
    
    def handle_usr2(self):
        """\
        SIGUSR2 handling.
        Creates a new master/worker set as a slave of the current
        master without affecting old workers. Use this to do live
        deployment with the ability to backout a change.
        """
        self.reexec()
        
    def handle_winch(self):
        "SIGWINCH handling"
        if os.getppid() == 1 or os.getpgrp() != os.getpid():
            self.log.info("graceful stop of workers")
            self.num_workers = 0
            self.kill_workers(signal.SIGQUIT)
        else:
            self.log.info("SIGWINCH ignored. Not daemonized")
    
    def wakeup(self):
        """\
        Wake up the arbiter by writing to the PIPE
        """
        try:
            os.write(self.PIPE[1], '.')
        except IOError, e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise
                    
    def halt(self, reason=None, exit_status=0):
        """ halt arbiter """
        self.stop()
        self.log.info("Shutting down: %s", self.master_name)
        if reason is not None:
            self.log.info("Reason: %s", reason)
        if self.pidfile is not None:
            self.pidfile.unlink()
        sys.exit(exit_status)
        
    def sleep(self):
        """\
        Sleep until PIPE is readable or we timeout.
        A readable PIPE means a signal occurred.
        """
        try:
            ready = select.select([self.PIPE[0]], [], [], 1.0)
            if not ready[0]:
                return
            while os.read(self.PIPE[0], 1):
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

        sig = signal.SIGQUIT
        if not graceful:
            sig = signal.SIGTERM
        limit = time.time() + self.timeout
        while self.WORKERS and time.time() < limit:
            self.kill_workers(sig)
            time.sleep(0.1)
            self.reap_workers()
        self.kill_workers(signal.SIGKILL)

    
    def on_reexec(self):
        """ methode launched when the server reexec """

    def reexec(self):
        """\
        Relaunch the master and workers.
        """

        # rename pidfile in old worker
        if self.pidfile is not None:
            self.pidfile.rename("%s.oldbin" % self.pidfile.fname)
        
        # fork to open a new master
        self.reexec_pid = os.fork()
        if self.reexec_pid != 0:
            self.master_name = "Old Master"
            return
            
        # go to the path on which we started
        os.chdir(self.START_CTX['cwd'])

        # pass any action before we launch a new masyet
        self.on_reexec()
            
        # then exec
        os.execvpe(self.START_CTX[0], self.START_CTX['args'], os.environ)
        

    def on_reload(self, conf, old_conf):
        """ method executed on reload """


    def reload(self):
        """ 
        used on HUP
        """
    
        # keep oldconf
        old_conf = self.conf.copy()
        
        # reload conf
        self.setup()

        # exec on reload hook
        self.on_reload(self.conf, old_conf)

        # spawn new workers with new app & conf
        for i in range(self.conf.get("num_workers", 1)):
            self.spawn_worker()
        
        # unlink pidfile
        if self.pidfile is not None:
            self.pidfile.unlink()

        # create new pidfile
        if self.conf.get("pidfile") is not None:
            self.pidfile = Pidfile(self.cfg.pidfile)
            self.pidfile.create(self.pid)
            
        # set new proc_name
        util._setproctitle("master [%s]" % self.proc_name)
        
        # manage workers
        self.manage_workers() 
        
    def murder_workers(self):
        """\
        Kill unused/idle workers
        """
        for (pid, worker) in self.WORKERS.items():
            try:
                diff = time.time() - os.fstat(worker.tmp.fileno()).st_ctime
                if diff <= self.timeout:
                    continue
            except ValueError:
                continue

            self.log.critical("WORKER TIMEOUT (pid:%s)", pid)
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
                if self.reexec_pid == wpid:
                    self.reexec_pid = 0
                else:
                    # A worker said it cannot boot. We'll shutdown
                    # to avoid infinite start/stop cycles.
                    exitcode = status >> 8
                    if exitcode == self.WORKER_BOOT_ERROR:
                        reason = "Worker failed to boot."
                        raise HaltServer(reason, self.WORKER_BOOT_ERROR)
                    worker = self.WORKERS.pop(wpid, None)
                    if not worker:
                        continue
                    worker.tmp.close()
        except OSError, e:
            if e.errno == errno.ECHILD:
                pass
    
    def manage_workers(self):
        """\
        Maintain the number of workers by spawning or killing
        as required.
        """
        if len(self.WORKERS.keys()) < self.num_workers:
            self.spawn_workers()

        workers = self.WORKERS.items()
        workers.sort(key=lambda w: w[1].age)
        while len(workers) > self.num_workers:
            (pid, _) = workers.pop(0)
            self.kill_worker(pid, signal.SIGQUIT)


    def pre_fork(self, worker):
        """ methode executed on prefork """

    def post_fork(self, worker):
        """ method executed after we forked a worker """
            
    def spawn_worker(self):
        self.worker_age += 1

        worker = self.worker_class(self.worker_age, self.pid,
                self.timeout/2.0, self.conf, self.worker_args)

        self.pre_fork(worker)
        pid = os.fork()
        if pid != 0:
            self.WORKERS[pid] = worker
            return

        # Process Child
        worker_pid = os.getpid()
        try:
            util._setproctitle("worker [%s]" % self.proc_name)
            self.log.info("Booting worker with pid: %s", worker_pid)
            self.post_fork(worker)
            worker.init_process()
            sys.exit(0)
        except SystemExit:
            raise
        except:
            self.log.exception("Exception in worker process:")
            if not worker.booted:
                sys.exit(self.WORKER_BOOT_ERROR)
            sys.exit(-1)
        finally:
            self.log.info("Worker exiting (pid: %s)", worker_pid)
            try:
                worker.tmp.close()
                self.cfg.worker_exit(self, worker)
            except:
                pass

    def spawn_workers(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        
        for i in range(self.num_workers - len(self.WORKERS.keys())):
            self.spawn_worker()

    def kill_workers(self, sig):
        """\
        Lill all workers with the signal `sig`
        :attr sig: `signal.SIG*` value
        """
        for pid in self.WORKERS.keys():
            self.kill_worker(pid, sig)
                   

    def kill_worker(self, pid, sig):
        """\
        Kill a worker
        
        :attr pid: int, worker pid
        :attr sig: `signal.SIG*` value
         """
        try:
            os.kill(pid, sig)
        except OSError, e:
            if e.errno == errno.ESRCH:
                try:
                    worker = self.WORKERS.pop(pid)
                    worker.tmp.close()
                    return
                except (KeyError, OSError):
                    return
            raise            


