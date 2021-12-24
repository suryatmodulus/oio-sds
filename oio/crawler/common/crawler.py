# Copyright (C) 2021 OVH SAS
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.

import random

from oio import ObjectStorageApi
from oio.blob.utils import check_volume_for_service_type
from oio.common.daemon import Daemon
from oio.common.easy_value import boolean_value, int_value
from oio.common.exceptions import ConfigurationException, InvalidPath
from oio.common.green import ratelimit, time, ContextPool
from oio.common.logger import get_logger
from oio.common.utils import paths_gen
from oio.crawler.meta2.loader import loadpipeline as meta2_loadpipeline
from oio.crawler.rawx.loader import loadpipeline as rawx_loadpipeline

LOAD_PIPELINES = {
    'rawx': rawx_loadpipeline,
    'meta2': meta2_loadpipeline
}


class CrawlerWorker(object):
    """
    Worker responsible for listing and processing items.
    """

    def __init__(self, conf, worker_id, logger=None):
        """
        Initialize a CrawlerWorker.

        :param conf: The configuration to be passed to the needed services.
        :param worker_id: A unique identifier for each worker.
        :param logger:
        """
        self.conf = conf
        self.logger = logger or get_logger(self.conf)
        self.running = True
        self.worker_id = worker_id

        self.wait_random_time_before_starting = boolean_value(
            self.conf.get('wait_random_time_before_starting'), False)
        self.scans_interval = int_value(self.conf.get('interval'), 1800)
        self.report_interval = int_value(self.conf.get('report_interval'), 300)
        self.max_scanned_per_second = int_value(
            self.conf.get('scanned_per_second'), 30)

        self.passes = 0
        self.successes = 0
        self.errors = 0
        self.start_time = 0
        self.last_report_time = 0
        self.scanned_since_last_report = 0

    def _reset_stats(self):
        """
        Resets all accumulated statistics except the number of passes.
        """
        self.successes = 0
        self.errors = 0

    def _report(self, tag, now):
        """
        Log a report containing all statistics.

        :param tag: One of three: starting, running, ended.
        :param now: The current timestamp to use in the report.
        """
        raise NotImplementedError

    def _list_items(self):
        """
        List all the items to be processed.
        """
        raise NotImplementedError

    def _process_item(self, item):
        """
        Perform the processing of an item.

        :param item: An item (listed by `_list_items`) to process.
        :returns: `True` if the item has been processed.
        """
        raise NotImplementedError

    def _end(self):
        """
        Run the code after everything has been processed
        and before the final report.
        """
        raise NotImplementedError

    def _wait_next_pass(self, start_crawl):
        """
        Wait for the remaining time before the next pass.

        :param tag: The start timestamp of the current pass.
        """
        crawling_duration = time.time() - start_crawl
        waiting_time_to_start = self.scans_interval - crawling_duration
        if waiting_time_to_start > 0:
            for _ in range(int(waiting_time_to_start)):
                if not self.running:
                    return
                time.sleep(1)
        else:
            self.logger.warning(
                '[%s] crawler duration=%d is higher than interval=%d',
                self.worker_id, crawling_duration, self.scans_interval)

    def report(self, tag, force=False):
        """
        Log the status of crawler.

        :param tag: One of three: starting, running, ended.
        :param force: Forces the report to be displayed even if the interval
            between reports has not been reached.
        """
        now = time.time()
        if not force and now - self.last_report_time < self.report_interval:
            return
        self._report(tag, now)
        self.last_report_time = now
        self.scanned_since_last_report = 0

    def crawl(self):
        """
        List and process all the items.
        """
        self.passes += 1
        self._reset_stats()

        self.report('starting', force=True)
        self.start_time = time.time()
        last_scan_time = 0

        for item in self._list_items():
            if not self.running:
                self.logger.info("Stop asked")
                break
            if not self._process_item(item):
                continue
            last_scan_time = ratelimit(
                last_scan_time, self.max_scanned_per_second)
            self.report('running')
        else:
            self._end()

        self.report('ended', force=True)

    def run(self):
        """
        Run passes successfully until worker is stopped.
        """
        if self.wait_random_time_before_starting:
            waiting_time_to_start = random.randint(0, self.scans_interval)
            self.logger.info('Wait %d secondes before starting',
                             waiting_time_to_start)
            for _ in range(waiting_time_to_start):
                if not self.running:
                    return
                time.sleep(1)
        while self.running:
            try:
                start_crawl = time.time()
                self.crawl()
                self._wait_next_pass(start_crawl)
            except Exception:
                self.logger.exception('[%s] Failed to crawl', self.worker_id)

    def stop(self):
        """
        Needed for gracefully stopping.
        """
        self.running = False


class Crawler(Daemon):
    """
    Daemon responsible for multiple workers who will list and process items.
    """

    SERVICE_TYPE = None

    def __init__(self, *args, **kwargs):
        """
        Initialize a Crawler.
        """
        super(Crawler, self).__init__(*args, **kwargs)
        self._init()
        self.workers = self._init_workers()
        self.pool = None
        if len(self.workers) > 1:
            self.pool = ContextPool(len(self.workers) - 1)

    def _init(self):
        """
        Initialize the attributes.
        """
        raise NotImplementedError

    def _init_workers(self):
        """
        Create the different worker instances.

        :returns: The list of these workers.
        """
        raise NotImplementedError

    def run(self, *args, **kwargs):
        """
        Run the workers until service is stopped.
        """
        self.logger.info("started %s crawler service", self.SERVICE_TYPE)

        for worker in self.workers[:-1]:
            self.pool.spawn(worker.run)
        if self.workers:
            self.workers[-1].run()
        if self.pool is not None:
            self.pool.waitall()

    def stop(self):
        """
        Call on all workers to stop.
        """
        self.logger.info("stop %s crawler asked", self.SERVICE_TYPE)
        for worker in self.workers:
            worker.stop()


class VolumeCrawlerWorker(CrawlerWorker):
    """
    Worker responsible for listing and processing files for a single volume.
    """

    SERVICE_TYPE = None

    def __init__(self, conf, volume_path, api=None, **kwargs):
        """
        Initialize a VolumeCrawlerWorker.

        :param conf: The configuration to be passed to the needed services.
        :param volume_path: The volume path to be crawled.
        :param api: An instance of oio.ObjectStorageApi.
        """
        self.volume = volume_path
        self.namespace, self.volume_id = check_volume_for_service_type(
            self.volume, self.SERVICE_TYPE)
        super(VolumeCrawlerWorker, self).__init__(
            conf, self.volume_id, **kwargs)

        self.invalid_paths = 0

        # This dict is passed to all filters called in the pipeline
        # of this worker
        self.app_env = {}
        self.app_env['api'] = api or ObjectStorageApi(
            self.namespace, logger=self.logger)
        self.app_env['volume_path'] = self.volume
        self.app_env['volume_id'] = self.volume_id

        self.pipeline = LOAD_PIPELINES[self.SERVICE_TYPE](
            conf.get('conf_file'), global_conf=self.conf, app=self)

    def _process_path(self, path):
        """
        Perform the processing of a file.

        :param path: The file path.
        :returns: `True` if the file has been processed.
        """
        raise NotImplementedError

    def cb(self, status, msg):
        raise NotImplementedError('cb not implemented')

    def _reset_stats(self):
        """
        Resets all accumulated statistics except the number of passes.
        """
        super(VolumeCrawlerWorker, self)._reset_stats()
        self.invalid_paths = 0
        # reset stats for each filter
        self.pipeline.reset_stats()

    def _report(self, tag, now):
        """
        Log a report containing all statistics.

        :param tag: One of three: starting, running, ended.
        :param now: The current timestamp to use in the report.
        """
        elapsed = (now - self.start_time) or 0.00001
        total = self.successes + self.errors
        since_last_rprt = (now - self.last_report_time) or 0.00001
        self.logger.info(
            '%(tag)s '
            'volume_id=%(volume_id)s '
            'elapsed=%(elapsed).02f '
            'pass=%(pass)d '
            'invalid_paths=%(invalid_paths)d '
            'errors=%(errors)d '
            'total_scanned=%(total_scanned)d '
            'rate=%(scan_rate).2f/s',
            {
                'tag': tag,
                'volume_id': self.volume_id,
                'elapsed': elapsed,
                'pass': self.passes,
                'invalid_paths': self.invalid_paths,
                'errors': self.errors,
                'total_scanned': total,
                'scan_rate': self.scanned_since_last_report / since_last_rprt,
            })

        for filter_name, stats in self.pipeline.get_stats().items():
            self.logger.info(
                '%(tag)s '
                'volume_id=%(volume_id)s '
                'filter=%(filter)s '
                '%(stats)s',
                {
                    'tag': tag,
                    'volume_id': self.volume_id,
                    'filter': filter_name,
                    'stats': ' '.join(('%s=%s' % (key, str(value))
                                       for key, value in stats.items()))
                }
            )

    def _list_items(self):
        """
        List all the files contained in the volume.
        """
        return paths_gen(self.volume)

    def _process_item(self, item):
        """
        Perform the processing of a file.

        :param item: The file path.
        :returns: `True` if the file has been processed.
        """
        self.logger.debug("crawl_volume current path: %s", item)
        try:
            return self._process_path(item)
        except InvalidPath:
            self.invalid_paths += 1
            return False

    def _end(self):
        """
        Do nothing after everything has been processed
        and before the final report.
        """
        pass


class VolumeCrawler(Crawler):
    """
    Daemon responsible for multiple workers who will list and process files
    for each volume.
    """

    def _init(self):
        """
        Initialize the attributes.
        """
        if not self.conf.get('conf_file'):
            raise ConfigurationException('Missing configuration path')
        self.api = ObjectStorageApi(self.conf['namespace'], logger=self.logger)

        self.volumes = list()
        for volume in self.conf.get('volume_list', '').split(','):
            volume = volume.strip()
            if volume and volume not in self.volumes:
                self.volumes.append(volume)
        if not self.volumes:
            raise ConfigurationException(
                f"No {self.SERVICE_TYPE} volumes provided to crawl!")
