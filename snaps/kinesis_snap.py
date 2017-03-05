import boto3
import logging
import time
import traceback

import utils


def postprocess_streams(streams):
    for stream in streams:
        stream['StreamCreationTimestamp'] = str(stream['StreamCreationTimestamp'])
    return streams


class KinesisSnapper(object):

    def __init__(self, awscontext, streams):
        self.ctx = awscontext
        self.streams = streams
        self._client = self._create_kinesis_client()

    def snap(self):
        logging.warn(
            'Start collecting meta data for region=%s', self.ctx.region)

        start = time.time()
        try:
            num_streams = self._do_snap()
        except Exception:
            logging.error(
                'Failed to collect meta data for region=%s error=%s',
                self.ctx.region, traceback.format_exc())
        else:
            logging.warn(
                'End of collecting meta data for region=%s discoverd=%d took=%s seconds',
                self.ctx.region, num_streams, time.time() - start)

    def _do_snap(self):
        # Discover
        if not self.streams:
            streams = self._list_streams()
        else:
            streams = self.streams.split(',')

        # Collect
        streams = self._describe_streams(streams)

        # Index
        with self.ctx.eventwriter as writer:
            writer.write(postprocess_streams(streams))

        return len(streams)

    def _create_kinesis_client(self):
        client = boto3.client(
            'kinesis',
            region_name=self.ctx.region,
            aws_access_key_id=self.ctx.access_key,
            aws_secret_access_key=self.ctx.secret_key,
        )
        return client

    def _list_streams(self):
        '''
        :return: a list of stream names in this region
        '''

        stream_names = []
        params = {'Limit': 20}
        while 1:
            response = self._client.list_streams(**params)
            if not utils.is_http_ok(response):
                msg = 'Failed to list Kinesis streams, errorcode={}'.format(
                    utils.http_code(response))
                logging.error(msg)
                raise Exception(msg)

            stream_names.extend(response.get('StreamNames', []))
            if response.get('HasHasMoreStreams'):
                params['ExclusiveStartStreamName'] = stream_names[-1]
            else:
                break

        return stream_names

    def _describe_streams(self, stream_names):
        '''
        :param stream_names: a list of stream names
        streams
        :return: a list of dict, each dict contains
        {
        'StreamName': 'string',
        'StreamARN': 'string',
        'StreamStatus': 'CREATING'|'DELETING'|'ACTIVE'|'UPDATING',
        'Shards': [
             {
                 'ShardId': 'string',
                 'ParentShardId': 'string',
                 'AdjacentParentShardId': 'string',
                 'HashKeyRange': {
                     'StartingHashKey': 'string',
                     'EndingHashKey': 'string'
                 },
                 'SequenceNumberRange': {
                     'StartingSequenceNumber': 'string',
                     'EndingSequenceNumber': 'string'
                 }
             },...]
        }
        '''

        streams = []
        for stream_name in stream_names:
            response = self._client.describe_stream(
                StreamName=stream_name)

            if not utils.is_http_ok(response):
                msg = 'Failed to describe Kinesis stream={} region={} errorcode={}'.format(
                    stream_name, self.ctx.region, utils.http_code(response))
                logging.error(msg)
                raise Exception(msg)

            if not response.get('StreamDescription'):
                continue

            streams.append(response['StreamDescription'])
        return streams


def add_params(subparsers):
    s3parser = subparsers.add_parser('kinesis')
    s3parser.add_argument(
        '--streams', dest='streams', required=False,
        help='Kinesis streams, separated by ","')


def new_snapper(awscontext, args):
    return KinesisSnapper(awscontext, args.streams)
