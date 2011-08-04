# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.

import mimetypes
import os

from http_parser.http import HttpStream
from http_parser.reader import SocketReader

from pistil import util
from pistil.tcp.arbiter import TcpArbiter
from pistil.tcp.gevent_worker import TcpGeventWorker

CURDIR = os.path.dirname(__file__)


def write_error(sock, status_int, reason, mesg):
    html = textwrap.dedent("""\
    <html>
      <head>
        <title>%(reason)s</title>
      </head>
      <body>
        <h1>%(reason)s</h1>
        %(mesg)s
      </body>
    </html>
    """) % {"reason": reason, "mesg": mesg}

    http = textwrap.dedent("""\
    HTTP/1.1 %s %s\r
    Connection: close\r
    Content-Type: text/html\r
    Content-Length: %d\r
    \r
    %s
    """) % (str(status_int), reason, len(html), html)
    write_nonblock(sock, http)



class HttpWorker(TcpGeventWorker):

    def handle(self, sock, addr):
        p = HttpStream(SocketReader(sock))

        path = p.path()

        if not path or path == "/":
            path = "index.html"
        
        if path.startswith("/"):
            path = path[1:]
        
        real_path = os.path.join(CURDIR, "static", path)
        if not os.path.exists(real_path):
            util.write_error(sock, 404, "Not found", real_path + " not found")
        else:
            ctype = mimetypes.guess_type(real_path)

            with open(real_path, 'r') as f:
                resp = "".join(["HTTP/1.1 200 OK\r\n", 
                                "Content-Type: " + ctype[0] + "\r\n",
                                "Connection: close\r\n\r\n",
                                f.read()])
                sock.sendall(resp)


def main():
    conf = {"address": ("127.0.0.1", 5000), "debug": True}
    print conf
    arbiter = TcpArbiter(conf, HttpWorker)
    arbiter.run()


if __name__ == '__main__':
    main()
