# coding: utf-8
import itertools
from heapq import heappush, heappop


class WaitingList(object):
    """Простая реализация PriorityQueue с возможностью удалить произвольный
    элемент из очереди.
    """
    DROPPED = '<dropped-item>'

    def __init__(self):
        self._queue = []
        self._entries = {}
        self._counter = itertools.count()

    def add(self, item, priority=0):
        """Добавить элемент в очередь.

        :param item:
            `any`, элемент, который нужно поместить в очередь.
        :param priority:
            `float`, приоритет.
        """
        if item in self._entries:
            self.drop(item)
        count = next(self._counter)
        entry = [priority, count, item]
        self._entries[item] = entry
        heappush(self._queue, entry)

    def pop(self, item=None):
        """Удалить элемент из очереди и вернуть его.

        :param item:
            `any`|`None`, найти и вернуть указанный элемент; если `None` --
            взять самый первый элемент.
        :return:
            `tuple`, (priority, item)

        :raises KeyError:
            элемент не найден.
        """
        if item is None:
            return self._pop()
        else:
            entry = self._entries.pop(item)
            priority, _, i = entry
            entry[-1] = self.DROPPED
            return priority, i

    def _pop(self):
        while self._queue:
            priority, count, entry = heappop(self._queue)
            if entry is not self.DROPPED:
                del self._entries[entry]
                return priority, entry
        raise KeyError("pop from an empty waiting list")

    def top(self):
        """Взять первый элемент из очереди не удаляя.

        :returns:
            `tuple`|None, (priority, item); None - если очередь пуста.
        """
        if not self._queue:
            return None
        priority, _, item = self._queue[0]
        if item is not self.DROPPED:
            return priority, item
        try:
            priority, item = self._pop()
        except KeyError:
            return None
        else:
            self.add(item, priority)
            return priority, item

    def drop(self, item):
        """Удалить элемент `item`.

        :param item:
            `any`, элемент.

        :raise KeyError:
            нет такого элемента.
        """
        entry = self._entries.pop(item)
        entry[-1] = self.DROPPED
