#!/usr/bin/python

import boto3
import Queue
import threading
import time
import traceback
import argparse


class S3Snapper(object):

    def __init__(self, access_key, secret_key, region, bucket_name, prefix):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.bucket_name = bucket_name
        self.prefix = prefix

    def snap(self):
        start = time.time()
        self._do_snap()
        print 'Done with collection for bucket_name={}, took={} seconds'.format(
            self.bucket_name, time.time() - start
        )

    def _do_snap(self):
        prefixes = self._discover_prefixes()
        print 'Discovered {} prefixes:\n {}'.format(
            len(prefixes), '\n'.join(prefixes))
        workers = []
        results_q = Queue.Queue()
        for prefix in prefixes:
            worker = threading.Thread(
                target=self._get_key_metas, args=(prefix, results_q))
            worker.start()
            workers.append(worker)

        with open('s3_keys.csv', 'w') as f:
            worker_done = 0
            while 1:
                key_metas = results_q.get()
                if key_metas is not None:
                    self._write_metas(f, key_metas)
                else:
                    worker_done += 1
                    if worker_done == len(workers):
                        break

        for worker in workers:
            worker.join()

    def _get_key_metas(self, prefix, result_q):
        try:
            self._do_get_key_metas(prefix, result_q)
        except Exception:
            print 'Failed to handle prefix={}, error={}'.format(
                prefix, traceback.format_exc())

        result_q.put(None)

    def _do_get_key_metas(self, prefix, result_q):
        client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        start_time = time.time()
        params = {
            'Bucket': self.bucket_name,
            'MaxKeys': 1000,
            'Prefix': prefix,
            'FetchOwner': False,
        }
        while 1:
            response = client.list_objects_v2(**params)
            next_token = response.get('NextContinuationToken')
            if not next_token or not response.get('Contents'):
                print 'Done with prefix={}, took={} seconds'.format(
                    prefix, time.time() - start_time)
                break

            if response.get('Contents'):
                result_q.put(response['Contents'])

            if next_token:
                params['ContinuationToken'] = next_token

    def _write_metas(self, f, key_metas):
        flattened = '\n'.join(
            ' '.join('{}={}'.format(key, val)
                     for key, val in key_meta.iteritems())
            for key_meta in key_metas)
        f.write(flattened)

    def _discover_prefixes(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        prefix = self.prefix
        while 1:
            response = client.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter='/',
                MaxKeys=1000,
                Prefix=prefix,
                FetchOwner=False,
            )

            common_prefixes = response.get('CommonPrefixes')
            if common_prefixes is None:
                return [prefix]

            if len(common_prefixes) == 1:
                prefix = common_prefixes[0]['Prefix']
            else:
                return [p['Prefix'] for p in common_prefixes]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--access_key', dest='access_key', required=True, help='AWS access key')
    parser.add_argument(
        '--secret_key', dest='secret_key', required=True, help='AWS secret_key')
    parser.add_argument(
        '--region', dest='region', required=True, help='AWS region')
    parser.add_argument(
        '--bucket_name', dest='bucket_name', required=True,
        help='S3 bucket name')
    parser.add_argument(
        '--prefix', dest='prefix', default='',
        help='S3 bucket prefix like AWSLogs/')

    args = parser.parse_args()

    snapper = S3Snapper(
        args.access_key, args.secret_key, args.region,
        args.bucket_name, args.prefix)
    snapper.snap()


if __name__ == '__main__':
    main()
