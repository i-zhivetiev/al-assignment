# coding: utf-8
"""
Вычислитель.
"""
import argparse
import logging
import signal
import socket
import time
from base64 import b16encode
from functools import partial
from random import random, uniform

from al_assignment.helpers import config
from al_assignment import message
from al_assignment.message import Ping

CONFIG_FILE = 'worker.json'


class Worker(object):
    """Класс вычислителя."""
    log = logging.getLogger('Worker')

    def __init__(self, cfg):
        """Инициализировать вычислитель.

        :param cfg:
            `dict`, словарь с настройками.
        """
        self.log.debug("configuration: %s", cfg)
        self._port = cfg['server']['port']
        self._client = (
            socket.gethostbyname(cfg['client']['host']),
            cfg['client']['port'],
        )

        self._calculation_range = (
            cfg['server']['min_calculation_period'],
            cfg['server']['max_calculation_period'],
        )

        self._breaking_probability = cfg['server']['breaking_probability']
        self._gray_out_period = cfg['server']['gray_out_period']

        self._ping_interval = cfg['server']['ping_interval']
        self._last_ping_time = 0

        self._is_running = False

        socket.setdefaulttimeout(self._ping_interval)
        self._dgram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def listen(self):
        """Стартовать UDP сервер."""
        self._dgram.bind((socket.gethostname(), self._port))

    def run(self):
        """Запустить процесс приёма задач и отправки сообщений ping."""
        self._is_running = True
        self.log.info("running")
        self.ping()
        while True:
            if self.is_broken:
                self.log.info("worker is broken")
                time.sleep(self._gray_out_period)
                self.ping()
            self.handle_dgram()

    @property
    def is_broken(self):
        """Выяснить поломан ли вычислитель."""
        return random() < self._breaking_probability

    def ping(self):
        """Послать сообщений ping клиенту."""
        ping = Ping().encode()
        now = time.time()
        if now - self._last_ping_time > self._ping_interval:
            self.log.info("ping %s", self._client)
            self._dgram.sendto(ping, self._client)
            self._last_ping_time = now

    def handle_dgram(self):
        """Обработать входящее сообщение. Если это задача -- выполнить и
        вернуть результат клиенту. Сообщения от неизвестных клиентов
        игнорируются.
        """
        try:
            data, addr = self._dgram.recvfrom(message.LEN)
        except socket.timeout:
            self.ping()
            return

        self.log.debug("got %s from %s", b16encode(data), addr)
        if addr != self._client:
            self.log.warning("unknown client: %s", addr)
            return

        try:
            task = message.decode(data)
        except message.MessageDecodeError as err:
            self.log.error("message decode error: %s", err)
            return

        if not message.is_task(task):
            self.log.warning("unexpected message type %s", task)
            return

        result = self.get_task_done(task)
        self._dgram.sendto(result.encode(), self._client)

    def get_task_done(self, task):
        """Выполнить задачу.

        :param task:
            `message.Task`, экземпляр задачи.

        :returns:
            `message.Result`, экземпляр результата вычисления.
        """
        self.log.info("trying to accomplish the task %s", task.task_id)
        calculation_period = uniform(*self._calculation_range)
        time.sleep(calculation_period)
        self.log.info(
            "the task %s is done in %s seconds",
            task.task_id,
            calculation_period,
        )
        return message.Result(task.task_id)

    def stop(self):
        """Остановить вычислитель."""
        self.log.info("stopping")
        self._is_running = False
        self._dgram.close()


def signal_handler(worker, num, _):
    """Обработчик сигналов. Останавливает вычислитель `worker` при получении
    `SIGINT` или `SIGTERM`. Параметр `frame` игнорируется.

    :param worker:
        `Worker`, экземпляр вычислителя.
    :param num:
        `int`, номер сигнала.
    """
    log = logging.getLogger('worker.signal_handler')
    log.info("got signal %s", num)
    worker.stop()
    exit(0)


def main():
    """Запустить процесс вычислителя."""
    arg_parser = argparse.ArgumentParser(description='Runs a worker process.')
    arg_parser.add_argument('-c', '--config', default=CONFIG_FILE)
    arg_parser.add_argument('-p', '--port', type=int)
    args = arg_parser.parse_args()

    cfg = config.read(args.config)
    if args.port:
        cfg['server']['port'] = args.port

    logging_config = cfg.pop("logging", {})
    logging.basicConfig(**logging_config)

    worker = Worker(cfg)

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, partial(signal_handler, worker))

    worker.listen()
    worker.run()


if __name__ == '__main__':
    main()
