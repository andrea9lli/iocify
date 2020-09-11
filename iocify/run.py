# -*- coding: utf-8 -*-
import argparse
import csv
import itertools
import multiprocessing
import os
from functools import partial

from iocify.utils import md5_checksum
from iocify.utils import sha1_checksum
from iocify.utils import sha256_checksum

manager = multiprocessing.Manager()
shared_list = manager.list()


def worker(hash_sum, filename):
    m = {
        'md5': md5_checksum,
        'sha1': sha1_checksum,
        'sha256': sha256_checksum,
    }
    global shared_list
    shared_list.append({'filename': filename,
                        hash_sum: m[hash_sum](filename)})


def work(**kwargs):
    with multiprocessing.Pool(int(kwargs['processes'])) as pool:
        walk = os.walk(kwargs['source'])
        fn_gen = itertools.chain.from_iterable(
            (os.path.join(root, file) for file in files) for root, dirs, files
            in walk)
        _h = [h for h in ['md5', 'sha1', 'sha256'] if kwargs[h] is True]
        if len(_h) == 0:
            raise Exception('Error: you must choose at least one hashing '
                            'algorithm (md5, sha1, sha256)')
        func = partial(worker, _h[0])
        _ = pool.map(func, fn_gen)

    outfile = kwargs['output']
    if not outfile.endswith('.csv'):
        outfile += '.csv'
    with open(outfile, mode='w') as f:
        fieldnames = ['filename', _h]
        writer = csv.DictWriter(f, fieldnames=fieldnames,
                                delimiter=kwargs['delimiter'])
        writer.writeheader()
        global shared_list
        for row in shared_list:
            writer.writerow(row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source',
                        help='root path of the source',
                        required=True)
    parser.add_argument('-o', '--output',
                        help='output file name (csv format)',
                        required=True)
    parser.add_argument('-d', '--delimiter',
                        help='csv delimiter (default: ",")',
                        default=',')
    parser.add_argument('--md5', action='store_true')
    parser.add_argument('--sha1', action='store_true')
    parser.add_argument('--sha256', action='store_true')
    parser.add_argument('-p', '--processes',
                        help='processes number',
                        default=4)

    args = parser.parse_args()
    work(**args.__dict__)
