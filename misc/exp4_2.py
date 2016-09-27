#!/usr/bin/env python
#
# http://docs.python.jp/3/library/multiprocessing.html#examples
#

# import time
# import random

from multiprocessing import Process, Queue, current_process, freeze_support

# Emulated by Thread. Maybe useful on testing?
# from multiprocessing.dummy import Process, Queue, current_process,\
#     freeze_support


def worker(input, output):
    '''Function run by worker processes'''
    try:
        for func, args in iter(input.get, 'STOP'):
            result = calculate(func, args)
            output.put(result)
    except KeyboardInterrupt:
        pass


def calculate(func, args):
    '''Function used to calculate result'''
    result = func(*args)
    return '%s says that %s%s = %s' % \
        (current_process().name, func.__name__, args, result)


def mul(a, b):
    '''Functions referenced by tasks'''
    # time.sleep(0.5*random.random())
    return a * b


def plus(a, b):
    '''Functions referenced by tasks'''
    # time.sleep(0.5*random.random())
    return a + b


def test():
    NUMBER_OF_PROCESSES = 4
    TASKS1 = [(mul, (i, 7)) for i in range(20)]
    TASKS2 = [(plus, (i, 8)) for i in range(10)]

    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    # Start worker processes
    for i in range(NUMBER_OF_PROCESSES):
        Process(target=worker, args=(task_queue, done_queue)).start()

    # Submit tasks
    for task in TASKS1:
        task_queue.put(task)

    # Get and print results
    print('Unordered results:')
    for i in range(len(TASKS1)):
        print('\t', done_queue.get())

    # Add more tasks using `put()`
    for task in TASKS2:
        task_queue.put(task)

    # Get and print some more results
    for i in range(len(TASKS2)):
        print('\t', done_queue.get())

    # Tell child processes to stop
    for i in range(NUMBER_OF_PROCESSES):
        task_queue.put('STOP')


if __name__ == '__main__':
    freeze_support()
    try:
        test()
    except KeyboardInterrupt:
        print('Keyboard Interrupt. Exitting')
