import boto3
import logging
import re
import os
import time
import traceback
import threading
import Queue
import cloudwatch_defaults


def is_http_ok(response):
    return response['ResponseMetadata']['HTTPStatusCode'] in (200, 201)


class DimensionExactMatcher():

    def __init__(self, re_value_dict):
        self.regexes = {}
        for key in re_value_dict:
            if not isinstance(re_value_dict[key], list):
                re_value_dict[key] = [re_value_dict[key]]

            self.regexes[key] = []
            for regex_str in re_value_dict[key]:
                if not regex_str.endswith('$'):
                    regex_str = regex_str + '$'
                self.regexes[key].append(re.compile(regex_str))

    def exact_match(self, dimension):
        dimension = {dim['Name']: dim['Value'] for dim in dimension}

        if len(self.regexes) != len(dimension):
            return False

        for key in self.regexes:
            if key not in dimension:
                return False

            for regex in self.regexes[key]:
                if isinstance(dimension[key], list):
                    matched = False
                    for value in dimension[key]:
                        if regex.match(value):
                            matched = True
                            break
                    if not matched:
                        return False
                else:
                    if not regex.match(dimension[key]):
                        return False
        return True


def get_dimension_filters(dimension_re_list):
    if not dimension_re_list:
        return []

    if not isinstance(dimension_re_list, list):
        dimension_re_list = [dimension_re_list]

    return [DimensionExactMatcher(re_value_dict)
            for re_value_dict in dimension_re_list]


class CloudWatchSnap(object):

    def __init__(self, awscontext, namespace, metric_names, dimension_regs):
        self.ctx = awscontext
        self.namespace = namespace
        self.metric_names = self._get_metric_names(metric_names)
        self.dimension_filters = get_dimension_filters(dimension_regs)
        self.common_log = 'region={} namespace={} metrics={}'.format(
            self.ctx.region, self.namespace, self.metric_names)

    def snap(self):
        logging.warn(
            'Start collecting meta data for %s', self.common_log)

        start = time.time()
        try:
            metric_num = self._do_snap()
        except Exception:
            logging.error(
                'Failed to collect meta data for %s error=%s',
                self.common_log, traceback.format_exc())
        else:
            logging.warn(
                'End of collecting meta data for %s discoverd=%d '
                'took=%s seconds',
                self.common_log, metric_num, time.time() - start)

    def _do_snap(self):
        # Collect
        workers = []
        results_q = Queue.Queue(10000)
        for metric_name in self.metric_names:
            worker = threading.Thread(
                target=self._collect_metric_meta, args=(metric_name, results_q))
            worker.start()
            workers.append(worker)

        # Index
        worker_done = 0
        metric_num = 0
        with self.ctx.eventwriter as writer:
            while 1:
                dim_metrics = results_q.get()
                if dim_metrics is not None:
                    metric_num += len(dim_metrics)
                    writer.write(dim_metrics)
                else:
                    worker_done += 1
                    if worker_done == len(workers):
                        break

        for worker in workers:
            worker.join()

        return metric_num

    def _collect_metric_meta(self, metric_name, results_q):
        start = time.time()

        try:
            metrics = self._list_metrics_by_metric_name(metric_name)
            if os.environ.get('cloudwatch_filter') in ('1', 'true', 'yes'):
                metrics = self._filter_invalid_dimensions(metrics)
            results_q.put(metrics)
        except Exception:
            msg = ('Failed to list metric for region={} namespace={} '
                   'metric_name={} error={}').format(
                       self.ctx.region, self.namespace, metric_name,
                       traceback.format_exc())
            logging.error(msg)
        else:
            logging.warn(
                'List metric for region=%s namespace=%s metric_name=%s, '
                'discovered=%s took=%s',
                self.ctx.region, self.namespace, metric_name, len(metrics),
                time.time() - start)

        results_q.put(None)

    def _list_metrics_by_metric_name(self, metric_name):
        client = boto3.client(
            'cloudwatch',
            region_name=self.ctx.region,
            aws_access_key_id=self.ctx.access_key,
            aws_secret_access_key=self.ctx.secret_key)

        all_metrics = []
        params = {
            'Namespace': self.namespace,
        }

        if metric_name:
            params['MetricName'] = metric_name

        while 1:
            response = client.list_metrics(**params)
            if not is_http_ok(response):
                logging.error(
                    'Failed to list_metrics for region=%s %s error=%s',
                    self.ctx.region, params, response)
                break

            for metric in response['Metrics']:
                if not metric['Dimensions']:
                    continue

                if not self._match_dimension(metric):
                    continue

                all_metrics.append(metric)
            token = response.get('NextToken')
            if token is None:
                break
            else:
                params['NextToken'] = token
        return all_metrics

    def _match_dimension(self, metric):
        if not self.dimension_filters:
            return True

        for matcher in self.dimension_regex_filters:
            if matcher.exact_match(metric['Dimensions']):
                return True
        return False

    def _filter_invalid_dimensions(self, metrics):
        # For now we only care EC2/EBS
        filter_map = {
            'AWS/EC2': {'service': 'ec2', 'func': self._filter_invalid_ec2_instances},
            'AWS/EBS': {'service': 'ec2', 'func': self._filter_invalid_ebs},
        }

        if self.namespace not in filter_map:
            return metrics

        client = boto3.client(
            filter_map[self.namespace]['service'],
            region_name=self.ctx.region,
            aws_access_key_id=self.ctx.access_key,
            aws_secret_access_key=self.ctx.secret_key,
        )

        return filter_map[self.namespace]['func'](client, metrics)

    def _filter_invalid_ec2_instances(self, client, metrics):
        valid_metrics, removed = self._do_filter_invalid_dimensions(
            client.describe_instances, 'Reservations', 'Instances',
            'InstanceId', metrics)
        if removed:
            valid_reserved_metrics, removed = self._do_filter_invalid_dimensions(
                client.describe_reserved_instances, 'ReservedInstances', '',
                'ReservedInstancesId', removed)
            valid_metrics.extend(valid_reserved_metrics)
        return valid_metrics

    def _filter_invalid_ebs(self, client, metrics):
        valid_metrics, _ = self._do_filter_invalid_dimensions(
            client.describe_volumes, 'Volumes', '', 'VolumeId', metrics)
        return valid_metrics

    def _do_filter_invalid_dimensions(
            self, describe_func, result_key, instance_key, id_key, metrics):
        try:
            exists = self._get_valid_dimensions(
                describe_func, result_key, instance_key, id_key)
        except Exception:
            logging.error(
                'Failed to get valid dimensions for %s, error=%s',
                self.common_log, traceback.format_exc())
            return metrics

        new_metrics, removed = [], []
        for m in metrics:
            for mid in m['Dimensions']:
                if mid['Value'] in exists:
                    new_metrics.append(m)
                    break
            else:
                removed.append(m)

        if logging.root.isEnabledFor(logging.warn):
            logging.warn(
                '%s total=%d, valid=%d, filtered=%d',
                self.common_log, len(metrics), len(new_metrics),
                len(metrics) - len(new_metrics))

            i, total = 0, len(removed)
            while 1:
                filtered_ids = ','.join(d['Value'] for m in removed[i: i + 100]
                                        for d in m['Dimensions'])
                if filtered_ids:
                    logging.warn('filtered_ids=%s', filtered_ids)

                if i >= total:
                    break
                i += 100

        return new_metrics, removed

    def _get_valid_dimensions(
            self, describe_func, result_key, instance_key, id_key):
        exists = set()
        params = {
            'DryRun': False
        }

        while 1:
            response = describe_func(**params)
            if not is_http_ok(response):
                logging.error(
                    'Failed to describe instances for %s, error=%s',
                    self.common_log, response)
                raise Exception(str(response))

            if not response.get(result_key):
                break

            for instance in response[result_key]:
                if instance_key:
                    for dim in instance[instance_key]:
                        exists.add(dim[id_key])
                else:
                    exists.add(instance[id_key])

            token = response.get('NextToken')
            if token is None:
                break
            else:
                params['NextToken'] = token
        return exists

    def _get_metric_names(self, metric_names):
        if not metric_names or metric_names == '.*':
            return cloudwatch_defaults.CLOUDWATCH_DEFAULT_METRICS[self.namespace]
        else:
            return [metric.strip() for metric in metric_names.split(',')]
