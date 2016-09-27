#!/usr/bin/env python
#
# http://docs.python.jp/3/library/multiprocessing.html#module-multiprocessing.dummy
#

from multiprocessing import Process, Queue


def f(q):
    q.put('X' * 1000000)


if __name__ == '__main__':
    print("Caution: this program will cause deadlock.")
    queue = Queue()
    p = Process(target=f, args=(queue,))
    p.start()
    p.join()                    # this deadlocks
    obj = queue.get()
