import boto3
import Queue
import threading
import time
import traceback
import logging


def postprocess_keys(key_metas):
    for key in key_metas:
        key['LastModified'] = str(key['LastModified'])
        del key['ETag']
    return key_metas


class S3Snapper(object):

    def __init__(self, awscontext, bucket_name, prefix):
        self.ctx = awscontext
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.common_log = 'region={} bucket_name={} prefix={}'.format(
            self.ctx.region, self.bucket_name, self.prefix)

    def snap(self):
        logging.warn(
            'Start collecting meta data for %s', self.common_log)

        start = time.time()
        try:
            num_keys = self._do_snap()
        except Exception:
            logging.error(
                'Failed to collect meta data for %s error=%s',
                self.common_log, traceback.format_exc())
        else:
            logging.warn(
                'End of collecting meta data for %s discoverd=%d took=%s seconds',
                self.common_log, num_keys, time.time() - start)

    def _do_snap(self):
        # Discover
        prefixes = self._discover_prefixes()
        logging.warn(
            'Discovered %s with count=%d sub-prefixes:\n %s',
            self.common_log, len(prefixes), '\n'.join(prefixes))

        # Collect
        workers = []
        results_q = Queue.Queue(10000)
        task_q = Queue.Queue()
        for prefix in prefixes:
            task_q.put(prefix)

        for i in xrange(self.ctx.concurrency):
            task_q.put(None)
            worker = threading.Thread(
                target=self._collect_key_metas, args=(task_q, results_q))
            worker.start()
            workers.append(worker)

        # Index
        worker_done = 0
        num_keys = 0
        with self.ctx.eventwriter as writer:
            while 1:
                key_metas = results_q.get()
                if key_metas is not None:
                    key_metas = postprocess_keys(key_metas)
                    num_keys += len(key_metas)
                    writer.write(key_metas)
                else:
                    worker_done += 1
                    if worker_done == self.ctx.concurrency:
                        break

        for worker in workers:
            worker.join()

        return num_keys

    def _collect_key_metas(self, task_q, result_q):
        while 1:
            prefix = task_q.get()
            if prefix is None:
                task_q.put(None)
                break

            try:
                self._do_collect(prefix, result_q)
            except Exception:
                logging.warn('Failed to handle %s error=%s',
                             self.common_log, traceback.format_exc())

        result_q.put(None)

    def _do_collect(self, prefix, result_q):
        client = boto3.client(
            's3',
            aws_access_key_id=self.ctx.access_key,
            aws_secret_access_key=self.ctx.secret_key,
        )

        params = {
            'Bucket': self.bucket_name,
            'MaxKeys': 1000,
            'Prefix': prefix,
            'FetchOwner': False,
        }

        start_time = time.time()
        num_keys = 0
        while 1:
            response = client.list_objects_v2(**params)
            next_token = response.get('NextContinuationToken')
            if response.get('Contents'):
                num_keys += len(response['Contents'])
                result_q.put(response['Contents'])

            if not next_token or not response.get('Contents'):
                logging.warn(
                    'Done with region=%s bucket_name=%s prefix=%s discoverd=%d '
                    'took=%s seconds',
                    self.ctx.region, self.bucket_name, prefix,
                    num_keys, time.time() - start_time)
                break

            if next_token:
                params['ContinuationToken'] = next_token

    def _discover_prefixes(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.ctx.access_key,
            aws_secret_access_key=self.ctx.secret_key,
        )

        all_discovered = []
        prefixes = [self.prefix]
        num_iteration = 0
        while 1:
            num_iteration += 1
            for prefix in prefixes:
                response = client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Delimiter='/',
                    MaxKeys=1000,
                    Prefix=prefix,
                    FetchOwner=True,
                )

                common_prefixes = response.get('CommonPrefixes')
                if common_prefixes is None:
                    all_discovered.append(prefix)
                    continue

                all_discovered.extend([p['Prefix'] for p in common_prefixes])

            if len(prefixes) != 1 and len(prefixes) == len(all_discovered):
                # No new prefixed discovered
                break

            if len(all_discovered) > 100 or num_iteration > 5:
                break
            else:
                prefixes = all_discovered
                all_discovered = []

        if all_discovered:
            return all_discovered
        else:
            return prefixes


def add_params(subparsers):
    s3parser = subparsers.add_parser('s3')
    s3parser.add_argument(
        '--bucket_name', dest='bucket_name', required=True,
        help='S3 bucket name')
    s3parser.add_argument(
        '--prefix', dest='prefix', default='',
        help='S3 bucket prefix like AWSLogs/')


def new_snapper(awscontext, args):
    return S3Snapper(awscontext, args.bucket_name, args.prefix)
