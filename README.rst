pistil
------


Simple multiprocessing toolkit. This is based on the `Gunicorn <http://gunicorn.org>`_ multiprocessing engine. 

This library allows you to supervise multiple type of workers and chain
supervisors. Gracefull, reload, signaling between workers is handled.  


Simple Arbiter launching one worker::

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

The same with different with the Pool arbiter. This time we send the
same worker on 3 os processes::
    
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

A common use for that pattern is a tcp server tjhat share the same
socket between them. For that purpose pistil provides the TcpArbiter and
TcpSyncWorker and the GeventTcpWorker to use with gevent.

Pistil allows you to mix diffrent kind of workers in an arbiter::

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

You can also chain arbiters:: 

    import time
    import urllib2

    from pistil.arbiter import Arbiter
    from pistil.worker import Worker
    from pistil.tcp.sync_worker import TcpSyncWorker
    from pistil.tcp.arbiter import TcpArbiter

    from http_parser.http import HttpStream
    from http_parser.reader import SocketReader

    class MyTcpWorker(TcpSyncWorker):

        def handle(self, sock, addr):
            p = HttpStream(SocketReader(sock))
            path = p.path()
            data = "welcome wold"
            sock.send("".join(["HTTP/1.1 200 OK\r\n", 
                            "Content-Type: text/html\r\n",
                            "Content-Length:" + str(len(data)) + "\r\n",
                             "Connection: close\r\n\r\n",
                             data]))


    class UrlWorker(Worker):

        def handle(self):
            f = urllib2.urlopen("http://localhost:5000")
            print f.read()

    class MyPoolArbiter(TcpArbiter):

        def on_init(self, conf):
            TcpArbiter.on_init(self, conf)
            # we return a spec
            return (MyTcpWorker, 30, "worker", {}, "http_welcome",)


    if __name__ == '__main__':
        conf = {"num_workers": 3, "address": ("127.0.0.1", 5000)}

        specs = [
            (MyPoolArbiter, 30, "supervisor", {}, "tcp_pool"),
            (UrlWorker, 30, "worker", {}, "grabber")
        ]

        arbiter = Arbiter(conf, specs)
        arbiter.run()


This examplelaunch a web server with 3 workers on port 5000 and another
worker fetching the welcome page hosted by this server::


    $ python examples/multiworker2.py 

    2011-08-08 00:05:42 [13195] [DEBUG] Arbiter master booted on 13195
    2011-08-08 00:05:42 [13196] [INFO] Booting grabber (worker) with pid: 13196
    ici
    2011-08-08 00:05:42 [13197] [INFO] Booting pool (supervisor) with pid: 13197
    2011-08-08 00:05:42 [13197] [DEBUG] Arbiter pool booted on 13197
    2011-08-08 00:05:42 [13197] [INFO] Listening at: http://127.0.0.1:5000 (13197)
    2011-08-08 00:05:42 [13198] [INFO] Booting worker (worker) with pid: 13198
    2011-08-08 00:05:42 [13199] [INFO] Booting worker (worker) with pid: 13199
    welcome world
    welcome world


More documentation is comming. See also examples in the examples/
folder.
