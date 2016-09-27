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
from logging import DEBUG
from logging import NullHandler
from logging.handlers import QueueHandler

from multiprocessing import Pipe, Process, Queue, freeze_support

import itertools
import os
import random
import string
from threading import Thread
import time


_null_logger = getLogger(__name__)
_null_logger.addHandler(NullHandler())


def _prepare_queue_logger(log_queue):
    # DEBUGレベルのルートロガーに対してQueueHandlerを指定し
    # 全てlog_queueに流し込む。
    # メインとなるプロセス側のlogger_threadが
    # ログを集約してくれる
    queue_logger = getLogger()
    queue_handler = QueueHandler(log_queue)
    queue_logger.addHandler(queue_handler)
    queue_logger.setLevel(DEBUG)
    queue_handler.setLevel(DEBUG)
    return queue_logger


def worker_process(input_queue, output_queue,
                   log_queue, out_dir_path, sleep_sec):
    try:
        logger = _prepare_queue_logger(log_queue)
        logger.debug('Start running worker_process (pid: {})'
                     .format(os.getpid()))
        for out_file_rel_path in iter(input_queue.get, 'STOP'):
            if sleep_sec:
                time.sleep(sleep_sec)
            out_path = os.path.join(out_dir_path, out_file_rel_path)
            # ディレクトリは出来ているはずだが念のため
            os.makedirs(os.path.basename(out_path),
                        exist_ok=True)
            f = open(out_path, 'w')
            f.write(''.join(random.choice(string.ascii_letters)
                            for l in range(1024)))
            f.close()
            output_queue.put(out_path)
        logger.debug('Finish running worker_process (pid: {})'
                     .format(os.getpid()))
    except KeyboardInterrupt:
        pass


def receiver_process(out_dir_path, result_queue, log_queue, conn):
    try:
        logger = _prepare_queue_logger(log_queue)
        logger.debug('Start running receiver_process (pid: {})'
                     .format(os.getpid()))
        results = []
        for out_abs_path in iter(result_queue.get, None):
            out_rel_path = os.path.relpath(out_abs_path, out_dir_path)
            logger.debug('Obtained "{}" from result_queue'
                         .format(out_rel_path))
            results.append(out_rel_path)
        conn.send(results)
        logger.debug('Finished running receiver_process (pid: {})'
                     .format(os.getpid()))
    except KeyboardInterrupt:
        pass


def host(num_processes, in_dir_path, out_dir_path,
         log_queue, worker_sleep_sec,
         *, logger=None):
    logger = logger or _null_logger
    logger.debug('num_processes: {}'.format(num_processes))
    logger.debug('in_dir_path: "{}"'.format(in_dir_path))
    logger.debug('out_dir_path: {})'.format(out_dir_path))
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

    workers = []
    request_queue = Queue()
    result_queue = Queue()

    # Workerプロセスの前にReceiverプロセスを起動しないと
    # result_queueが詰まる
    parent_conn, child_conn = Pipe()
    receiver_args = (out_dir_path, result_queue, log_queue, child_conn)
    receiver = Process(target=receiver_process,
                       args=receiver_args)
    receiver.start()

    for i in range(num_processes):
        args = (request_queue, result_queue, log_queue,
                out_dir_path, worker_sleep_sec)
        p = Process(target=worker_process, args=args)
        workers.append(p)
        p.start()

    # output_queueにputされるはずのファイル数
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
        request_queue.put('STOP')

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

    if len(results) != num_files:
        logger.error('Received results size seems wrong: {} != {}'
                     .format(len(results), num_files))

    successful = check(in_dir_path, out_dir_path, results,
                       logger=logger)
    if successful:
        logger.debug('in_dir_path and out_dir_path seem to have'
                     ' same structure. Looks good.')
    return successful


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


def logger_thread(log_queue):
    '''\
    複数プロセスから送られてくるLogRecordを集約するThreadの本体
    log_queueから'STOP'を受け取ると終了する
    '''
    for record in iter(log_queue.get, 'STOP'):
        logger = getLogger(__name__)
        logger.handle(record)


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
                        help=('Path to watch'))
    parser.add_argument('-n', '--num-processes', type=int,
                        default=5,
                        help='Number of worker processes')
    parser.add_argument('-s', '--worker-sleep-sec', type=float,
                        default=None,
                        help='Sleep time (in sec) of each work')
    args = parser.parse_args()
    logger = getLogger(__name__)
    handler = StreamHandler()
    if args.debug:
        handler.setLevel(DEBUG)
        logger.setLevel(DEBUG)
    else:
        handler.setLevel(args.log.upper())
        logger.setLevel(args.log.upper())
    logger.addHandler(handler)
    # e.g. '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handler.setFormatter(Formatter('%(asctime)s - %(process)d - %(message)s'))
    try:
        logger.info('Start running')
        log_queue = Queue()
        log_thread = Thread(target=logger_thread, args=(log_queue,))
        log_thread.start()
        host(args.num_processes,
             os.path.abspath(args.in_dir_path),
             os.path.abspath(args.out_dir_path),
             log_queue,
             args.worker_sleep_sec,
             logger=logger)
        # sentinelを送りlog_threadを終了させる
        log_queue.put('STOP')
        log_thread.join()
        logger.info('Finished running')
    except KeyboardInterrupt:
        logger.info('Keyboard Interrupt occured. Exitting')


if __name__ == '__main__':
    freeze_support()
    main()
