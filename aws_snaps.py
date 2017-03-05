#!/usr/bin/python

import argparse
import logging

import snaps
import aws_context as ctx
import event_writer as ew
import boto3


def init():
    boto3.setup_default_session()
    boto3.DEFAULT_SESSION._session.get_component('data_loader')
    boto3.DEFAULT_SESSION._session.get_component('event_emitter')
    boto3.DEFAULT_SESSION._session.get_component('endpoint_resolver')
    boto3.DEFAULT_SESSION._session.get_component('credential_provider')


def main():
    logging.basicConfig(level=logging.WARNING)
    init()

    parser = argparse.ArgumentParser()

    # Global common args
    parser.add_argument(
        '--access_key', dest='access_key', required=True, help='AWS access key')
    parser.add_argument(
        '--secret_key', dest='secret_key', required=True, help='AWS secret_key')
    parser.add_argument(
        '--region', dest='region', required=True, help='AWS region')
    parser.add_argument(
        '--target_file', dest='fname', required=False,
        help='File name to store the meta data collected')
    parser.add_argument(
        '--concurrency', dest='concurrency', required=False,
        type=int, default=16, help='number of threads')

    subparsers = parser.add_subparsers(dest="cmd")
    for mod in snaps.snaps:
        mod.add_params(subparsers)

    args = parser.parse_args()

    if not args.fname:
        args.fname = '{}_meta.json'.format(args.cmd)

    writer = ew.JsonEventWriter(args.fname)
    context = ctx.AWSContext(
        writer, args.access_key, args.secret_key,
        args.region, args.concurrency)

    snap_map = {
        's3': snaps.s3_snap.new_snapper,
        'cloudwatch': snaps.cloudwatch_snap.new_snapper,
        'kinesis': snaps.kinesis_snap.new_snapper,
    }

    snapper = snap_map[args.cmd](context, args)
    snapper.snap()


if __name__ == '__main__':
    main()
