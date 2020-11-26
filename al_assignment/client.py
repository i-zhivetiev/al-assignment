# coding: utf-8
"""
Клиент.
"""
import argparse
import logging
import signal
import socket
import time
from base64 import b16encode
from collections import namedtuple
from functools import partial
from random import uniform

from al_assignment.helpers import config
from al_assignment import message
from al_assignment.message import Task

CONFIG_FILE = 'client.json'

SentTask = namedtuple('SentTask', ['task_id', 'send_time'])


class Client(object):
    """Класс клиента."""
    log = logging.getLogger('Client')

    def __init__(self, cfg):
        """Инициализировать клиент.

        :param cfg:
            `dict`, словарь с настройками.
        """
        self.log.debug("configuration: %s", cfg)
        self._port = cfg['server']['port']

        self._worker = (
            socket.gethostbyname(cfg['worker']['host']),
            cfg['worker']['port'],
        )

        self._send_task_range = (
            cfg['server']['min_send_task_period'],
            cfg['server']['max_send_task_period'],
        )

        self._result_timeout = cfg['server']['request_timeout']

        self._is_running = False

        socket.setdefaulttimeout(self._result_timeout)
        self._dgram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._sent_task = None
        self._sent_tasks_count = 0
        self._task_durations = []

    def listen(self):
        """Запустить UDP сервер."""
        self._dgram.bind((socket.gethostname(), self._port))

    def run(self):
        """Запустить процесс генерации задач и обработки результатов."""
        self.log.info("running")
        self._is_running = True

        while True:
            self.log.info("waiting for the moment to send a task")
            time.sleep(self.time_to_wait)
            self.send_task()
            self.wait_for_result()

    @property
    def time_to_wait(self):
        """Время, которое нужно подождать до генерации очередной задачи."""
        return uniform(*self._send_task_range)

    def send_task(self):
        """Отослать задачу вычислителю."""
        task = Task()
        self._sent_task = SentTask(task.task_id, time.time())

        self.log.info("send task: %s", task.task_id)
        self._dgram.sendto(task.encode(), self._worker)

        self._sent_tasks_count += 1

    def wait_for_result(self):
        """Дождаться сообщения с результатом и вызвать обработчик."""
        waiting_for_result = True
        while waiting_for_result:
            if self.is_time_out():
                self.log.warning("timeout")
                break

            try:
                data, addr = self._dgram.recvfrom(message.LEN)
            except socket.timeout:
                self.log.warning("timeout")
                break

            self.log.debug("got %s from %s", b16encode(data), addr)

            try:
                msg = message.decode(data)
            except message.MessageDecodeError as err:
                self.log.error("Message decode error: %s", err)
                continue

            if addr != self._worker:
                self.log.warning("unknown worker: %s", addr)
                continue

            if message.is_result(msg):
                waiting_for_result = not self.handle_result(msg)

    def handle_result(self, result):
        """Обработать результат. Устаревшие результаты отбрасываются.

        :param result:
            `message.Result`, экземпляр результата.

        :returns:
            `bool`: результат успешно обработан.
        """
        if result.task_id != self._sent_task.task_id:
            self.log.info("got some stale result")
            return False

        end = time.time()
        start = self._sent_task.send_time
        self._task_durations.append(end - start)

        self.log.info("got a result on %s", result.task_id)
        self.log.info("duration of the task: %s", self._task_durations[-1])

        return True

    def is_time_out(self):
        """Узнать, вышло ли время ожидания результата.

        :returns:
            `bool`, время вышло.
        """
        if self._sent_task is None:
            return False
        return time.time() - self._sent_task.send_time > self._result_timeout

    def stop(self):
        """Остановить клиент."""
        self.log.info("stopping")
        self._is_running = False
        self._dgram.close()
        self.dump_stats()

    def dump_stats(self):
        """Вывести статистику выполнения задач в лог."""
        min_time = None
        max_time = None
        avg_time = None
        uncompleted_tasks = self._sent_tasks_count
        completed_tasks = len(self._task_durations)

        if completed_tasks > 0:
            min_time = min(self._task_durations)
            max_time = max(self._task_durations)
            avg_time = sum(self._task_durations) / completed_tasks
            uncompleted_tasks = self._sent_tasks_count - completed_tasks

        self.log.info("Total sent tasks: %s", self._sent_tasks_count)
        self.log.info("Completed tasks: %s", len(self._task_durations))
        self.log.info("Uncompleted tasks: %s", uncompleted_tasks)
        self.log.info("Minimum request time: %s", min_time)
        self.log.info("Maximum request time: %s", max_time)
        self.log.info("Average request time: %s", avg_time)


def signal_handler(client, num, _):
    """Обработчик сигналов. Останавливает клиента `client` при получении
    `SIGINT` или `SIGTERM`. Параметр `frame` игнорируется.

    :param client:
        `Client`, экземпляр клиента.
    :param num:
        `int`, номер сигнала.
    """
    log = logging.getLogger('client.signal_handler')
    log.info("got signal %s", num)
    client.stop()
    exit(0)


def main():
    """Запустить процесс клиента."""
    arg_parser = argparse.ArgumentParser(description='Runs a client process.')
    arg_parser.add_argument('-c', '--config', default=CONFIG_FILE)
    arg_parser.add_argument('-p', '--port', type=int)
    args = arg_parser.parse_args()

    cfg = config.read(args.config)
    if args.port:
        cfg['server']['port'] = args.port

    logging_config = cfg.pop("logging", {})
    logging.basicConfig(**logging_config)

    client = Client(cfg)

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, partial(signal_handler, client))

    client.listen()
    client.run()


if __name__ == '__main__':
    main()
