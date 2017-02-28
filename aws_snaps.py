#!/usr/bin/python

import argparse

import s3_snap
import cloudwatch_snap
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
        '--target_file', dest='fname', required=True,
        default='aws_s3_keys.csv',
        help='File name to store the meta data collected')

    subparsers = parser.add_subparsers(dest="cmd")

    # S3 specific, TODO: refactor out
    s3parser = subparsers.add_parser('s3')
    s3parser.add_argument(
        '--bucket_name', dest='bucket_name', required=True,
        help='S3 bucket name')
    s3parser.add_argument(
        '--prefix', dest='prefix', default='',
        help='S3 bucket prefix like AWSLogs/')

    # CloudWatch specific, TODO: refactor out
    cloudwatch_parser = subparsers.add_parser('cloudwatch')
    cloudwatch_parser.add_argument(
        '--namespace', dest='namespace', required=True,
        help='CloudWatch namespace like AWS/EC2')
    cloudwatch_parser.add_argument(
        '--metrics', dest='metrics', default='',
        help='CloudWatch metrics like CPUCreditBalance,CPUCreditUsage')
    cloudwatch_parser.add_argument(
        '--dimension_filter', dest='dim_filter_rex', default='',
        help='CloudWatch dimension filter')

    args = parser.parse_args()

    writer = ew.CsvEventWriter(args.fname)
    context = ctx.AWSContext(
        writer, args.access_key, args.secret_key, args.region)

    if args.cmd == 's3':
        snapper = s3_snap.S3Snapper(context, args.bucket_name, args.prefix)
    elif args.cmd == 'cloudwatch':
        snapper = cloudwatch_snap.CloudWatchSnap(
            context, args.namespace, args.metrics, args.dim_filter_rex)
    else:
        assert 0
    snapper.snap()


if __name__ == '__main__':
    main()
