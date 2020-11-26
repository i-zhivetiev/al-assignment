# coding: utf-8
"""
Диспетчер.
"""
import argparse
import logging
import signal
import socket
import time
from base64 import b16encode
from collections import namedtuple, deque
from functools import partial
from heapq import heappop, heappush

from al_assignment import message
from al_assignment.helpers import config
from al_assignment.helpers.waiting_list import WaitingList

CONFIG_FILE = 'dispatcher.json'

EnqueuedTask = namedtuple('EnqueuedTask', ['received_from', 'task'])
TaskQueueItem = namedtuple('TaskQueueItem', ['time_to_send', 'enqueued_task'])

AwaitedResult = namedtuple('AwaitedResult', ['client', 'worker', 'task'])
EnqueuedResult = namedtuple('EnqueuedResult', ['client', 'result'])


class ResultTracker(object):
    """Трекер ожидаемых результатов. Помогает отслеживать задачи, отправленные
    вычислителям.

    Хранит:
     - экземпляр задачи;
     - адрес клиента от которого пришла;
     - задача, вычислитель, которому эта задача направлена;
     - таймаут, метку времени, по достижении которой задачу можно считать
       устаревшей.
    """
    log = logging.getLogger('Dispatcher.ResultTracker')

    def __init__(self):
        self._tasks = dict()
        self._result_queue = WaitingList()

    def add(self, awaited_result, timeout=0):
        """Добавить результат в список ожидания.

        :param awaited_result:
            `AwaitedResult`, экземпляр ожидаемого результата.
        :param timeout:
            `float`, таймаут.
        """
        task_id = awaited_result.task.task_id
        self._tasks[task_id] = awaited_result
        self._result_queue.add(task_id, timeout)

    def pop(self, task_id):
        """Убрать задачу из списка ожидания и вернуть её.

        :returns:
            `tuple`, (timeout, awaited_result).

        :raises KeyError:
            нет такой задачи.
        """
        awaited_result = self._tasks.pop(task_id)
        priority, _ = self._result_queue.pop(task_id)
        return priority, awaited_result

    def top(self):
        """Вернуть ожидаемый результат с наименьшим значением `timeout`.

        :returns:
            `tuple`|None, (timeout, awaited_result); None -- если
            очередь пуста.
        """
        if len(self._tasks) == 0:
            return None
        priority, task_id = self._result_queue.top()
        awaited_result = self._tasks.get(task_id)
        return priority, awaited_result


class Dispatcher(object):
    """Класс диспетчера."""
    log = logging.getLogger('Dispatcher')

    def __init__(self, cfg):
        """Инициализировать диспетчера.

        :param cfg:
            `dict`, словарь с настройками.
        """
        self.log.debug("configuration: %s", cfg)

        self._port = cfg['server']['port']
        self._worker_timeout = cfg['server']['worker_timeout']

        self._task_queue = []
        self._result_queue = deque()
        self._result_tracker = ResultTracker()
        self._workers = {}

        self._dgram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._dgram.settimeout(self._worker_timeout)

        self._is_running = False

    def listen(self):
        """Запустить UDP сервер."""
        self._dgram.bind((socket.gethostname(), self._port))

    def run(self):
        """Запустить процесс обработки сообщений от клиентов и вычислителей."""
        self._is_running = True
        self.log.info("running")
        while True:
            self.handle_tasks_to_send()
            self.send_results()
            self.handle_waiting_list()
            self.handle_dgram()

    def handle_tasks_to_send(self):
        """Обработать задачи, которые необходимо послать на выполнение.

        Берёт первый доступный вычислитель и отправляет очередную
        задачу из очереди на отправку. Если доступных вычислителей нет --
        помещает задачу обратно в очередь с таймаутом.
        """
        now = time.time()
        while self._task_queue:
            if now < self._task_queue[0].time_to_send:
                break
            queue_item = heappop(self._task_queue)
            worker = self.get_worker()
            if worker is None:
                self.log.warning("there are no workers")
                self.enqueue_task(
                    client=queue_item.enqueued_task.received_from,
                    task=queue_item.enqueued_task.task,
                    timeout=now + self._worker_timeout,
                )
                break
            self.send_task_to(queue_item.enqueued_task, worker)

    def send_task_to(self, enqueued_task, worker):
        """Отослать задачу вычислителю.

        :param enqueued_task:
            `EnqueuedTask`, экземпляр задачи из очереди.
        :param worker:
            `tuple`, (host, port), адрес вычислителя.
        """
        client = enqueued_task.received_from
        task = enqueued_task.task
        self.log.info("send task %s to the worker %s", task, worker)
        self._dgram.sendto(task.encode(), worker)

        now = time.time()
        self._result_tracker.add(
            awaited_result=AwaitedResult(client, worker, task),
            timeout=now + self._worker_timeout,
        )

    def handle_dgram(self):
        """Обработать входящую датаграмму."""
        self.log.debug("waiting for a dgram")
        try:
            data, addr = self._dgram.recvfrom(message.LEN)
        except socket.timeout:
            self.log.debug("timeout, taking the next iteration")
            return

        self.log.debug("got dgram %s from %s", b16encode(data), addr)

        try:
            msg = message.decode(data)
        except message.MessageDecodeError as err:
            self.log.error("Message decode error: %s", err)
            return

        if message.is_ping(msg):
            self.log.info("got a ping from the worker %s", addr)
            self.update_workers(addr)
        elif message.is_result(msg):
            self.handle_result(msg, addr)
        elif message.is_task(msg):
            self.enqueue_task(addr, msg)
        else:
            self.log.warning("got unknown type of the message: %s", msg)

    def update_workers(self, addr):
        """Обновить список вычислителей.

        Добавляет новый вычислитель к списку активных. Или обновляет время
        последней активности, если вычислитель уже есть в списке.

        :param addr:
            `tuple`, (host, port), адрес вычислителя.
        """
        self.log.debug("update worker list: %s", addr)
        self._workers[addr] = time.time()
        self.log.debug("workers: %s", self._workers)

    def handle_result(self, result, worker):
        """Обработать результат.

        :param result:
            `message.Result`, экземпляр сообщения с результатом.
        :param worker:
            `tuple`, (host, port), адрес вычислителя, от которого пришёл
            результат.
        """
        self.log.info("got a result %s from the worker %s", result, worker)
        _, awaited_result = self._result_tracker.pop(result.task_id)
        enqueued_result = EnqueuedResult(
            client=awaited_result.client,
            result=result,
        )
        self._result_queue.append(enqueued_result)
        self.update_workers(worker)

    def enqueue_task(self, client, task, timeout=None):
        """Поместить задачу в очередь на отправку.

        :param client:
            `tuple`, адрес клиента, от которого получена задача.
        :param task:
            `message.Task`, экземпляр задачи.
        :param timeout:
            `float`, таймаут на отправку.
        """
        self.log.info(
            "enqueue a task %s from the client %s",
            task.task_id, client,
        )
        if timeout is None:
            timeout = time.time()
        enqueued_task = EnqueuedTask(client, task)
        queue_item = TaskQueueItem(timeout, enqueued_task)
        heappush(self._task_queue, queue_item)
        self.log.debug("the task queue length: %s", len(self._task_queue))

    def get_worker(self):
        """Вернуть первый доступный вычислитель и удалить его из списка
        доступных.

        Вычислитель считается доступным, если с момента его последней
        активности прошло не больше, чем `worker_timeout`. Недоступные
        вычислители не возвращаются и удаляются из списка.

        :returns:
            `tuple`|None, (host, port) адрес вычислителя, None -- если
            доступных вычислителей нет.
        """
        if not self._workers:
            return None
        now = time.time()
        while self._workers:
            worker, was_active_at = self._workers.popitem()
            if now - was_active_at <= self._worker_timeout:
                return worker

    def handle_waiting_list(self):
        """Обработать лист ожидания результатов.

        Все задачи, время ожидания результата которых вышло, помещаются в
        очередь на отправку вычислителям с нулевым таймаутом.
        """
        top_item = self._result_tracker.top()
        if top_item is None:
            return
        now = time.time()
        timeout, awaited_result = top_item
        while now > timeout:
            task_id = awaited_result.task.task_id

            try:
                _, outdated_task = self._result_tracker.pop(task_id)
            except KeyError:
                self.log.error("no such task in tracker: %s", task_id)
            else:
                self.log.warning(
                    "enqueue uncompleted task %s from %s",
                    outdated_task.client,
                    outdated_task.task,
                )
                self.enqueue_task(
                    client=outdated_task.client,
                    task=outdated_task.task,
                )

            top_item = self._result_tracker.top()
            if top_item is None:
                return
            timeout, awaited_result = top_item

    def send_results(self):
        """Отправить результаты клиентам."""
        while self._result_queue:
            enqueued_result = self._result_queue.pop()
            self.log.info("send result to %s", enqueued_result.client)
            self._dgram.sendto(
                enqueued_result.result.encode(),
                enqueued_result.client,
            )

    def stop(self):
        """Остановить диспетчер."""
        self.log.info("stopping")
        self._is_running = False
        self._dgram.close()


def signal_handler(dispatcher, num, _):
    """Обработчик сигналов. Останавливает диспетчер `dispatcher` при получении
    `SIGINT` или `SIGTERM`. Параметр `frame` игнорируется.

    :param dispatcher:
        `Dispatcher`, экземпляр диспетчера.
    :param num:
        `int`, номер сигнала.
    """
    log = logging.getLogger('dispatcher.signal_handler')
    log.info("got signal %s", num)
    dispatcher.stop()
    exit(0)


def main():
    """Запустить процесс диспетчера."""
    arg_parser = argparse.ArgumentParser(
        description='Runs a dispatcher process.'
    )
    arg_parser.add_argument('-c', '--config', default=CONFIG_FILE)
    args = arg_parser.parse_args()

    cfg = config.read(args.config)

    logging_config = cfg.pop("logging", {})
    logging.basicConfig(**logging_config)

    dispatcher = Dispatcher(cfg)

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, partial(signal_handler, dispatcher))

    dispatcher.listen()
    dispatcher.run()


if __name__ == '__main__':
    main()
