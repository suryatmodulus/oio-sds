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


from collections import namedtuple
from os.path import isfile

from oio.blob.operator import ChunkOperator
from oio.blob.utils import check_volume
from oio.common.exceptions import ConfigurationException, NotFound, \
    OioException, OrphanChunk, ServiceBusy, UnrecoverableContent, \
    VolumeException
from oio.common.green import get_watchdog, time
from oio.common.easy_value import int_value
from oio.common.http_urllib3 import get_pool_manager
from oio.conscience.client import ConscienceClient
from oio.crawler.common.crawler import Crawler, CrawlerWorker
from oio.rdir.client import RdirClient

RawxService = namedtuple('RawxService', ('status', 'last_time'))


class RdirWorker(CrawlerWorker):
    """
    Worker responsible for listing and processing chunk entries
    for a single rdir volume.
    """

    def __init__(self, conf, volume_path, pool_manager=None, **kwargs):
        """
        Initializes an RdirWorker.

        :param conf: The configuration to be passed to the needed services
        :param volume_path: The volume path to be indexed
        :param pool_manager: A connection pool manager. If none is given, a
            new one with a default size of 10 will be created.
        """
        # To keep compatibility with the old configuration
        if 'chunks_per_second' in conf:
            if 'scanned_per_second' not in conf:
                conf['scanned_per_second'] = conf['chunks_per_second']
            del conf['chunks_per_second']

        self.volume_path = volume_path
        self.namespace, self.volume_id = check_volume(self.volume_path)
        super(RdirWorker, self).__init__(conf, self.volume_id, **kwargs)

        self.hash_width = self.conf.get('hash_width')
        if not self.hash_width:
            raise ConfigurationException('No hash_width specified')
        self.hash_depth = self.conf.get('hash_depth')
        if not self.hash_depth:
            raise ConfigurationException('No hash_depth specified')
        self.conscience_cache = int_value(
            self.conf.get('conscience_cache'), 30)

        self.repaired = 0
        self.orphans = 0
        self.unrecoverable = 0

        self._rawx_service = RawxService(status=False, last_time=0)

        if not pool_manager:
            pool_manager = get_pool_manager(pool_connections=10)
        self.index_client = RdirClient(conf, logger=self.logger,
                                       pool_manager=pool_manager)
        self.watchdog = get_watchdog()
        self.conscience_client = ConscienceClient(self.conf,
                                                  logger=self.logger,
                                                  pool_manager=pool_manager)
        self.chunk_operator = ChunkOperator(self.conf, logger=self.logger,
                                            watchdog=self.watchdog)

    def _check_rawx_up(self):
        now = time.time()
        status, last_time = self._rawx_service
        # If the conscience has been requested in the last X seconds, return
        if now < last_time + self.conscience_cache:
            return status

        status = True
        try:
            data = self.conscience_client.all_services('rawx')
            # Check that all rawx are UP
            # If one is down, the chunk may be still rebuildable in the future
            for srv in data:
                tags = srv['tags']
                addr = srv['addr']
                up = tags.pop('tag.up', 'n/a')
                if not up:
                    self.logger.debug('service %s is down, rebuild may not'
                                      'be possible', addr)
                    status = False
                    break
        except OioException:
            status = False

        self._rawx_service = RawxService(status, now)
        return status

    def error(self, container_id, chunk_id, msg, *format):
        self.logger.error('volume_id=%s container_id=%s chunk_id=%s ' + msg,
                          self.volume_id, container_id, chunk_id, *format)

    def _build_chunk_path(self, chunk_id):
        chunk_path = self.volume_path
        for i in range(int(self.hash_depth)):
            start = chunk_id[i * int(self.hash_width):]
            chunk_path = '{}/{}'.format(chunk_path,
                                        start[:int(self.hash_width)])
        chunk_path = '{}/{}'.format(chunk_path, chunk_id)
        return chunk_path

    def _reset_stats(self):
        """
        Resets all accumulated statistics except the number of passes.
        """
        super(RdirWorker, self)._reset_stats()
        self.orphans = 0
        self.repaired = 0
        self.unrecoverable = 0

    def _report(self, tag, now):
        """
        Log a report containing all statistics.

        :param tag: One of three: starting, running, ended.
        :param now: The current timestamp to use in the report.
        """
        elapsed = (now - self.start_time) or 0.00001
        total = self.successes + self.errors
        since_last_rprt = (now - self.last_report_time) or 0.00001
        since_last_rprt = (now - self.last_report_time) or 0.00001
        self.logger.info(
            '%(tag)s '
            'volume_id=%(volume_id)s '
            'elapsed=%(elapsed).02f '
            'pass=%(pass)d '
            'repaired=%(repaired)d '
            'errors=%(errors)d '
            'unrecoverable=%(unrecoverable)d '
            'orphans=%(orphans)d '
            'total_scanned=%(total_scanned)d '
            'rate=%(scan_rate).2f/s',
            {
                'tag': tag,
                'volume_id': self.volume_id,
                'elapsed': elapsed,
                'pass': self.passes,
                'repaired': self.repaired,
                'errors': self.errors,
                'unrecoverable': self.unrecoverable,
                'orphans': self.orphans,
                'total_scanned': total,
                'scan_rate': self.scanned_since_last_report / since_last_rprt,
            })

    def _list_items(self):
        """
        List all the chunk entries to be processed.
        """
        try:
            chunks = self.index_client.chunk_fetch(self.volume_id)
            for chunk in chunks:
                yield chunk
        except (ServiceBusy, VolumeException, NotFound) as err:
            self.logger.warning('Service busy or not available: %s', err)

    def _process_item(self, item):
        """
        Perform the processing of a chunk entry.

        :param item: A chunk entry to process.
        :returns: `True` if the chunk entry has been processed.
        """
        container_id, chunk_id, value = item
        chunk_path = self._build_chunk_path(chunk_id)
        self.logger.debug("current chunk_id=%s volume_id=%s",
                          chunk_id, self.volume_id)

        try:
            if not isfile(chunk_path):
                self.chunk_operator.rebuild(
                    container_id, value['content_id'], chunk_id,
                    rawx_id=self.volume_id)
                self.repaired += 1
            self.successes += 1
        except OioException as exc:
            self.errors += 1
            if isinstance(exc, UnrecoverableContent):
                self.unrecoverable += 1
                if self._check_rawx_up():
                    self.error(container_id, chunk_id, '%s, action required',
                               exc)
            elif isinstance(exc, OrphanChunk):
                # Note for later: if it an orphan chunk, we should tag it and
                # increment a counter for stats. Another tool could be
                # responsible for those tagged chunks.
                self.orphans += 1
            else:
                self.error(container_id, chunk_id, 'failed to process, err=%s',
                           exc)
        except Exception:
            self.errors += 1
            self.logger.exception('Failed to process chunk')
        self.scanned_since_last_report += 1
        return True

    def _end(self):
        """
        Do nothing after everything has been processed
        and before the final report.
        """
        pass


class RdirCrawler(Crawler):
    """
    Daemon responsible for multiple workers who will list
    and process chunk entries for each rdir volume.
    """

    SERVICE_TYPE = 'rdir'

    def _init(self):
        """
        Initialize the attributes.
        """
        self.volumes = list()
        for volume in self.conf.get('volume_list', '').split(','):
            volume = volume.strip()
            if volume and volume not in self.volumes:
                self.volumes.append(volume)
        if not self.volumes:
            raise ConfigurationException(
                f"No {self.SERVICE_TYPE} volumes provided to crawl!")

    def _init_workers(self):
        """
        Create the different worker instances.

        :returns: The list of these workers.
        """
        pool_manager = get_pool_manager()
        return [
            RdirWorker(self.conf, volume, logger=self.logger,
                       pool_manager=pool_manager)
            for volume in self.volumes]
