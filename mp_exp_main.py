#!/usr/bin/env python

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
from logging import getLogger, StreamHandler, Formatter, NullHandler
from logging import DEBUG

from multiprocessing import Process, Queue, freeze_support

import itertools
import os
import random
import string


_null_logger = getLogger(__name__)
_null_logger.addHandler(NullHandler())


def worker(input_queue, output_queue, out_dir_path):
    try:
        for out_file_rel_path in iter(input_queue.get, 'STOP'):
            out_path = os.path.join(out_dir_path, out_file_rel_path)
            out_dir_path = os.path.dirname(out_path)
            os.makdirs(out_dir_path, exist_ok=True)
            f = open(out_path, 'w')
            f.write(''.join(random.choice(string.ascii_letters)
                            for l in range(1024)))
            f.close()
            output_queue.put(out_path)
    except KeyboardInterrupt:
        pass


def host(num_processes, in_dir_path, out_dir_path,
         *, logger=None):
    logger = logger or _null_logger
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
    os.mkdir(out_dir_path)

    processes = set()
    request_queue = Queue()
    result_queue = Queue()

    for i in range(num_processes):
        p = Process(target=worker,
                    args=(request_queue, result_queue, out_dir_path))
        processes.add(p)
        p.start()

    for (dirpath, dirnames, filenames) in os.walk(in_dir_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(file_path, in_dir_path)
            logger.debug('Pushing "{}" to request_queue'
                         .format(rel_path))
            request_queue.put(rel_path)

    for p in processes:
        request_queue.put('STOP')

    for p in processes:
        logger.debug('Joining Process "{}"'.format(p.name))
        p.join()

    result_queue.close()
    results = []
    while not result_queue.empty():
        rel_path = result_queue.get_nowait()
        logger.debug('Obtained "{}" from result_queue'
                     .format(rel_path))
        results.append(rel_path)
    check(in_dir_path, out_dir_path, results, logger=logger)


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
            rel_path = os.path.relpath(file_path, in_dir_path)
            out_dir_path_lst.append(rel_path)
    logger.debug('in_dir_path_lst size: {}'
                 .format(in_dir_path_lst))
    logger.debug('out_dir_path_lst size: {}'
                 .format(out_dir_path_lst))
    logger.debug('results size: {}'
                 .format(results))
    in_dir_path_lst.sort()
    out_dir_path_lst.sort()
    results.sort()
    if not (in_dir_path_lst == out_dir_path_lst == results):
        logger.error('Result is inconsistent')
        for (index, (i, o, r)) in enumerate(
                itertools.zip_longest(in_dir_path_lst,
                                      out_dir_path_lst,
                                      results)):
            logger.error('{}: "{}", "{}", "{}"'
                         .format(index, i, o, r))


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
    handler.setFormatter(Formatter('%(asctime)s %(message)s'))
    try:
        logger.info('Start running')
        host(args.num_processes,
             os.path.abspath(args.in_dir_path),
             os.path.abspath(args.out_dir_path),
             logger=logger)
        logger.info('Finished running')
    except KeyboardInterrupt:
        logger.info('Keyboard Interrupt occured. Exitting')


if __name__ == '__main__':
    # 主にWindows環境でspawnの準備をするために必須
    freeze_support()

