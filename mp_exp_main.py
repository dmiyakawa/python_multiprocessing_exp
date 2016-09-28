#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''\
Workerプロセスを経由してファイルを生成するデモ

in_dir_pathをos.walk()で巡回し、同じ構成のツリー構造を
out_dir_pathへ作る。in_dir_pathからみた相対パスと
out_dir_pathから見た相対パスは完全一致する。
in_dir_pathの巡回はホストで行い、out_dir_pathに対する
操作を指定されたワーカで分担する。

ファイルコンテンツは1KBの無意味なアスキー文字列。
'''

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from logging import getLogger, StreamHandler, Formatter
from logging import DEBUG, WARN
from logging import NullHandler
from logging.handlers import QueueHandler

from multiprocessing import Pipe, Process, Queue, freeze_support

import itertools
import os
from queue import Empty
import random
import string
from threading import Thread
import time


_null_logger = getLogger(__name__)
_null_logger.addHandler(NullHandler())


class CustomError(Exception):
    pass


def _human_readable_time(elapsed_sec):
    elapsed_int = int(elapsed_sec)
    days = elapsed_int // 86400
    hours = (elapsed_int // 3600) % 24
    minutes = (elapsed_int // 60) % 60
    seconds = elapsed_int % 60
    if days > 0:
        return '{} days {:02d}:{:02d}:{:02d}'.format(days, hours,
                                                     minutes, seconds)
    return '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)


def _prepare_queue_logger(log_queue):
    # DEBUGレベルのルートロガーに対してQueueHandlerを指定し
    # 全てlog_queueに流し込む。
    # メインとなるプロセス側のlogger_taskが
    # ログを集約してくれる
    queue_logger = getLogger()
    queue_handler = QueueHandler(log_queue)
    queue_logger.addHandler(queue_handler)
    queue_logger.setLevel(DEBUG)
    queue_handler.setLevel(DEBUG)
    return queue_logger


def logger_task(log_queue):
    '''\
    複数プロセスから送られてくるLogRecordを集約するタスク本体。
    Threadでの実行を前提とする。
    log_queueからNoneを受け取ると終了する
    '''
    for record in iter(log_queue.get, None):
        logger = getLogger(__name__)
        logger.handle(record)


def worker_task(request_queue, result_queue,
                out_dir_path, sleep_sec,
                *, is_process=True, log_queue=None, logger=None):
    '''\
    request_queueから作成するべきファイルの相対パスを受取り
    out_dir_path下に同一相対パスの1KBのランダムなファイルを作成し、
    作成したファイルの絶対パスをresult_queueへ送る。
    '''
    try:
        if is_process:
            if log_queue:
                logger = _prepare_queue_logger(log_queue)
            else:
                logger = _null_logger
        else:
            if logger:
                if type(logger) is str:
                    logger = getLogger(logger)
            else:
                logger = _null_logger
        logger.debug('A new worker started (pid: {}, is_process: {})'
                     .format(os.getpid(), is_process))
        for out_file_rel_path in iter(request_queue.get, None):
            if sleep_sec:
                time.sleep(sleep_sec)
            out_abs_path = os.path.join(out_dir_path, out_file_rel_path)
            # ディレクトリは出来ているはずだが念のため
            os.makedirs(os.path.basename(out_abs_path), exist_ok=True)
            f = open(out_abs_path, 'w')
            f.write(''.join(random.choice(string.ascii_letters)
                            for l in range(1024)))
            f.close()
            result_queue.put(out_abs_path)
        logger.debug('Worker exitting (pid: {})'.format(os.getpid()))
    except KeyboardInterrupt:
        logger.debug('Worker aborting (pid: {})'.format(os.getpid()))


def receiver_task(out_dir_path, result_queue, conn,
                  *, is_process=True, log_queue=None, logger=None):
    '''\
    result_queueから絶対パスを受取り、resultsリストを構築する。
    最終的にresultsリストをconn.send()で呼び出し元に送信する。

    Note: Windows環境ではloggerはシリアライズ出来ない(Pickleしようとすると
    失敗する)そのため、logger=loggerという指定も出来ない。
    回避策として、logger引数には文字列を受け取れるようにする。
    文字列の場合、その文字列を用いてlogger = getLogger(logger)を実行する
    '''
    try:
        if is_process:
            if log_queue:
                logger = _prepare_queue_logger(log_queue)
            else:
                logger = _null_logger
        else:
            if logger:
                if type(logger) is str:
                    logger = getLogger(logger)
            else:
                logger = _null_logger
        logger.debug('A new receiver started (pid: {}, is_process: {})'
                     .format(os.getpid(), is_process))
        results = []
        for out_abs_path in iter(result_queue.get, None):
            out_rel_path = os.path.relpath(out_abs_path, out_dir_path)
            logger.debug('Obtained "{}" from result_queue'
                         .format(out_rel_path))
            results.append(out_rel_path)
        logger.debug('Sending results (len: {})'.format(len(results)))
        conn.send(results)
        logger.debug('Receiver exitting (pid: {})'.format(os.getpid()))
    except KeyboardInterrupt:
        logger.debug('Receiver aborting (pid: {})'.format(os.getpid()))


def host(num_processes, in_dir_path, out_dir_path,
         log_queue, worker_sleep_sec, receiver_is_process,
         *, logger=None):
    logger = logger or _null_logger
    logger.info('num_processes: {}'.format(num_processes))
    logger.info('in_dir_path: "{}"'.format(in_dir_path))
    logger.info('out_dir_path: "{}"'.format(out_dir_path))
    logger.info('worker_sleep_sec: {}'.format(worker_sleep_sec))
    logger.info('receiver_is_process: {}'.format(receiver_is_process))

    if not os.path.exists(in_dir_path):
        logger.error('"{}" does not exist'.format(in_dir_path))
        return
    if not os.path.isdir(in_dir_path):
        logger.error('"{}" does is not a directory'.format(in_dir_path))
        return
    # out_dir_path はあってはならない
    if os.path.exists(out_dir_path):
        logger.error('"{}" exists'.format(out_dir_path))
        return
    logger.debug('Creating "{}"'.format(out_dir_path))
    os.mkdir(out_dir_path)

    request_queue = Queue()
    result_queue = Queue()

    # Workerプロセスの前にReceiverを起動しないとresult_queueが詰まる
    parent_conn, child_conn = Pipe()
    receiver_args = (out_dir_path, result_queue, child_conn)
    receiver_kwargs = {'is_process': receiver_is_process,
                       'log_queue': log_queue,
                       'logger': __name__}
    receiver = None
    workers = []
    try:
        if receiver_is_process:
            logger.debug('Using Process for receiver')
            receiver = Process(target=receiver_task,
                               args=receiver_args,
                               kwargs=receiver_kwargs)
        else:
            logger.debug('Using Thread for receiver')
            receiver = Thread(target=receiver_task,
                              args=receiver_args,
                              kwargs=receiver_kwargs)
        receiver.start()

        for i in range(num_processes):
            args = (request_queue, result_queue, out_dir_path,
                    worker_sleep_sec)
            kwargs = {'is_process': True,
                      'log_queue': log_queue,
                      'logger': __name__}
            p = Process(target=worker_task, args=args, kwargs=kwargs)
            workers.append(p)
            logger.debug('Starting a new worker {}'.format(p.name))
            p.start()

        # result_queueにputされるはずのファイル数
        num_files = 0
        for (dirpath, dirnames, filenames) in os.walk(in_dir_path):
            # ディレクトリはhost側で作成してしまう
            # Workerで作れないこともないが、マルチプロセスで順序が
            # 逆転すると、ディレクトリを作成するプロセスの前に
            # そのディレクトリにファイルを作成するプロセスが実行される
            # 懸念があり、ディレクトリを作成するプロセスの意義が微妙。
            for dirname in dirnames:
                dir_to_create = os.path.join(
                    out_dir_path,
                    os.path.relpath(
                        os.path.join(dirpath, dirname),
                        in_dir_path))
                os.mkdir(dir_to_create)
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(file_path, in_dir_path)
                logger.debug('Pushing "{}" to request_queue'
                             .format(rel_path))
                request_queue.put(rel_path)
                num_files += 1

        # sentinelを送信してWorkerに終了するよう指示する
        for p in workers:
            request_queue.put(None)

        # Workerプロセスの終了を待つ
        for p in workers:
            logger.debug('Joining Process "{}"'.format(p.name))
            p.join()
        logger.debug('All worker processes ended.')

        # Workerプロセスが終了した時点でresult_queueにsentinelを送る
        # これによりReceiverプロセスに終了するよう指示する
        result_queue.put(None)
        results = parent_conn.recv()
        receiver.join()
    except KeyboardInterrupt:
        logger.info('Keyboard Interrupt occured during work.'
                    ' Aborting children first.')
        # sentinelを送る
        for p in workers:
            request_queue.put(None)
        result_queue.put(None)
        log_queue.put(None)
        for p in workers:
            p.join()
        logger.debug('Retrieving data from queues')
        while not request_queue.empty():
            try:
                request_queue.get_nowait()
            except Empty:
                break
        while not result_queue.empty():
            try:
                result_queue.get_nowait()
            except Empty:
                break
        if parent_conn.poll():
            logger.debug('Pipe contains data. Retrieving it')
            parent_conn.recv()
        receiver.join()
        logger.info('Children aborted.')
        raise

    if len(results) == num_files:
        logger.info('Total num of files processed: {}'.format(num_files))
    else:
        logger.error('Received results size seems wrong: {} != {}'
                     .format(len(results), num_files))
    if check(in_dir_path, out_dir_path, results, logger=logger):
        logger.debug('in_dir_path and out_dir_path seem to have'
                     ' exactly same structure. Looks perfect.')


def check(in_dir_path, out_dir_path, results,
          *, logger=None):
    '''
    in_dir_path, out_dir_path, resultsが完全一致していることを確認する
    '''
    logger = logger or _null_logger
    in_dir_path_lst = []
    out_dir_path_lst = []
    for (dirpath, dirnames, filenames) in os.walk(in_dir_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(file_path, in_dir_path)
            in_dir_path_lst.append(rel_path)

    for (dirpath, dirnames, filenames) in os.walk(out_dir_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(file_path, out_dir_path)
            out_dir_path_lst.append(rel_path)
    in_dir_path_lst.sort()
    out_dir_path_lst.sort()
    results.sort()
    if not (in_dir_path_lst == out_dir_path_lst == results):
        logger.error('Result is inconsistent')
        logger.error('in_dir_path_lst size: {}'
                     .format(len(in_dir_path_lst)))
        logger.error('out_dir_path_lst size: {}'
                     .format(len(out_dir_path_lst)))
        logger.error('results size: {}'
                     .format(len(results)))
        for (index, (i, o, r)) in enumerate(
                itertools.zip_longest(in_dir_path_lst,
                                      out_dir_path_lst,
                                      results)):
            logger.error('{}: "{}", "{}", "{}"'
                         .format(index, i, o, r))
        return False
    return True


def main():
    parser = ArgumentParser(description=(__doc__),
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('in_dir_path',
                        help='Path to input directory')
    parser.add_argument('out_dir_path',
                        help='Path to output directory')
    parser.add_argument('--log',
                        default='INFO',
                        help=('Set log level. e.g. DEBUG, INFO, WARN'))
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Same as --log DEBUG')
    parser.add_argument('-w', '--warn', action='store_true',
                        help='Same as --log WARN')
    parser.add_argument('-n', '--num-processes', type=int,
                        default=5,
                        help='Number of worker processes')
    parser.add_argument('-s', '--worker-sleep-sec', type=float,
                        default=None,
                        help='Sleep time (in sec) of each work')
    parser.add_argument('--receiver-is-process', action='store_true',
                        help='Use process instead of thread for receiver')
    args = parser.parse_args()
    logger = getLogger(__name__)
    logger.propagate = False  # ルートロガーにログを伝播させない
    handler = StreamHandler()
    if args.debug:
        handler.setLevel(DEBUG)
        logger.setLevel(DEBUG)
    elif args.warn:
        handler.setLevel(WARN)
        logger.setLevel(WARN)
    else:
        handler.setLevel(args.log.upper())
        logger.setLevel(args.log.upper())
    logger.addHandler(handler)
    # e.g. '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handler.setFormatter(Formatter('%(asctime)s - %(process)d - %(message)s'))
    try:
        num_processes = args.num_processes
        in_dir_path = os.path.abspath(args.in_dir_path)
        out_dir_path = os.path.abspath(args.out_dir_path)
        worker_sleep_sec = args.worker_sleep_sec
        receiver_is_process = args.receiver_is_process
        logger.info('Start running')
        started = time.time()
        log_queue = Queue()
        log_thread = Thread(target=logger_task, args=(log_queue,))
        log_thread.start()
        host(num_processes, in_dir_path, out_dir_path, log_queue,
             worker_sleep_sec, receiver_is_process, logger=logger)
        # sentinelを送りlog_threadを終了させる
        log_queue.put(None)
        log_thread.join()
        ended = time.time()
        logger.info('Finished running (Elapsed {})'
                    .format(_human_readable_time(ended - started)))
    except KeyboardInterrupt:
        logger.info('Keyboard Interrupt occured. Exitting')


if __name__ == '__main__':
    freeze_support()
    main()
