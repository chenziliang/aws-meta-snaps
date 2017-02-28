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
        logging.info(
            'Start collecting meta data for %s', self.common_log)

        start = time.time()
        try:
            self._do_snap()
        except Exception:
            logging.error(
                'Failed to collect meta data for %s error=%s',
                self.common_log, traceback.format_exc())

        logging.info(
            'End of collecting meta data for %s took=%s seconds',
            self.common_log, time.time() - start)

    def _do_snap(self):
        # Discover
        prefixes = self._discover_prefixes()
        logging.info(
            'Discovered %s with count=%d sub-prefixes:\n %s',
            self.common_log, len(prefixes), '\n'.join(prefixes))

        # Collect
        workers = []
        results_q = Queue.Queue(10000)
        for prefix in prefixes:
            worker = threading.Thread(
                target=self._collect_key_metas, args=(prefix, results_q))
            worker.start()
            workers.append(worker)

        # Index
        worker_done = 0
        with self.ctx.eventwriter as writer:
            while 1:
                key_metas = results_q.get()
                if key_metas is not None:
                    key_metas = postprocess_keys(key_metas)
                    writer.write(key_metas)
                else:
                    worker_done += 1
                    if worker_done == len(workers):
                        break

        for worker in workers:
            worker.join()

    def _collect_key_metas(self, prefix, result_q):
        try:
            self._do_collect(prefix, result_q)
        except Exception:
            logging.info('Failed to handle %s error=%s',
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
        while 1:
            response = client.list_objects_v2(**params)
            next_token = response.get('NextContinuationToken')
            if not next_token or not response.get('Contents'):
                logging.info(
                    'Done with region=%s bucket_name=%s prefix=%s took=%s seconds',
                    self.ctx.region, self.bucket_name, prefix,
                    time.time() - start_time)
                break

            if response.get('Contents'):
                result_q.put(response['Contents'])

            if next_token:
                params['ContinuationToken'] = next_token

    def _discover_prefixes(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.ctx.access_key,
            aws_secret_access_key=self.ctx.secret_key,
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
