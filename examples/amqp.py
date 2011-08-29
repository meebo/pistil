#-*- coding: utf-8 -*-
"""
A simple worker with a AMQP consumer.

This example shows how to implement a simple AMQP consumer
based on `Kombu <http://github.com/ask/kombu>`_ and shows you
what different kind of workers you can put to a arbiter
to manage the worker lifetime, event handling and shutdown/reload szenarios.
"""
import sys
import time
import socket
import logging
from pistil.arbiter import Arbiter
from pistil.worker import Worker
from kombu.connection import BrokerConnection
from kombu.messaging import Exchange, Queue, Consumer, Producer


CONNECTION = ('localhost', 'guest', 'default', '/')

log = logging.getLogger(__name__)


class AMQPWorker(Worker):

    queues = [
        {'routing_key': 'test',
         'name': 'test',
         'handler': 'handle_test'
        }
    ]

    _connection = None

    def handle_test(self, body, message):
        log.debug("Handle message: %s" % body)
        message.ack()

    def handle(self):
        log.debug("Start consuming")
        exchange = Exchange('amqp.topic', type='direct', durable=True)
        self._connection = BrokerConnection(*CONNECTION)
        channel = self._connection.channel()

        for entry in self.queues:
            log.debug("prepare to consume %s" % entry['routing_key'])
            queue = Queue(entry['name'], exchange=exchange,
                          routing_key=entry['routing_key'])
            consumer = Consumer(channel, queue)
            consumer.register_callback(getattr(self, entry['handler']))
            consumer.consume()

        log.debug("start consuming...")
        while True:
            try:
                self._connection.drain_events()
            except socket.timeout:
                log.debug("nothing to consume...")
                break
        self._connection.close()

    def run(self):
        while self.alive:
            try:
                self.handle()
            except Exception:
                self.alive = False
                raise

    def handle_quit(self, sig, frame):
        if self._connection is not None:
            self._connection.close()
        self.alive = False

    def handle_exit(self, sig, frame):
        if self._connection is not None:
            self._connection.close()
        self.alive = False
        sys.exit(0)


if __name__ == "__main__":
    conf = {}
    specs = [(AMQPWorker, None, "worker", {}, "test")]
    a = Arbiter(conf, specs)
    a.run()
