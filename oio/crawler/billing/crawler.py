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

import pika

from oio.account.backend_fdb import AccountBackendFdb
from oio.crawler.common.crawler import Crawler, CrawlerWorker


class BillingWorker(CrawlerWorker):
    """
    Worker responsible for listing and processing buckets stats
    to send billing messages.
    """

    def __init__(self, conf, **kwargs):
        """
        Initializes an BillingWorker.

        :param conf: The configuration to be passed to the needed services
        """
        super(BillingWorker, self).__init__(conf, 'billing', **kwargs)

        self.backend = AccountBackendFdb(self.conf, self.logger)
        self.limit_accounts = 1000
        self.limit_buckets = 1000
        self.batch_size = 50
        self.batch = []

        self.amqp_url = self.conf['amqp_url']
        self.connection = None
        self.channel = None

    def _list_accounts(self):
        # TODO(adu): accounts = self.backend.list_accounts(self.limit_accounts)
        accounts = self.backend.list_accounts()
        for account in accounts:
            yield account

    def _list_buckets(self):
        for account in self._list_accounts():
            marker = ""
            while marker is not None:
                buckets, marker = self.backend.list_buckets(
                    account, limit=self.limit_buckets, marker=marker)
                for bucket in buckets:
                    yield bucket

    def _connect(self, url):
        """
        Returns an AMQP BlockingConnection and a channel for the provided URL
        :param url: AMQP URL
        """
        url_param = pika.URLParameters(url)
        self.logger.info("Connecting to %s", url_param)

        connection = pika.BlockingConnection(url_param)
        try:
            channel = connection.channel()
        except Exception:
            if connection.is_open:
                connection.close()
            raise
        else:
            return connection, channel

    def _send_batch(self):
        print(self.batch)
        if not self.batch:
            return
        self.batch.clear()

    def _reset_stats(self):
        """
        Resets all accumulated statistics except the number of passes.
        """
        super(BillingWorker, self)._reset_stats()
        # TODO

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
            'elapsed=%(elapsed).02f '
            'pass=%(pass)d '
            'errors=%(errors)d '
            'total_scanned=%(total_scanned)d '
            'rate=%(scan_rate).2f/s',
            {
                'tag': tag,
                'elapsed': elapsed,
                'pass': self.passes,
                'errors': self.errors,
                'total_scanned': total,
                'scan_rate': self.scanned_since_last_report / since_last_rprt,
            })
        # TODO

    def _list_items(self):
        """
        List all the buckets stats to be processed.
        """
        return self._list_buckets()

    def _process_item(self, item):
        """
        Perform the processing of a bucket stats.

        :param item: A bucket stats to process.
        :returns: `True` if the bucket stats has been processed.
        """
        print(item)
        self.batch.append(item)
        if len(self.batch) >= self.batch_size:
            self._send_batch()
        return True

    def _end(self):
        """
        Do nothing after everything has been processed
        and before the final report.
        """
        self._send_batch()


class BillingCrawler(Crawler):
    """
    Daemon responsible for multiple workers who will list
    and process buckets stats to send billing messages.
    """

    SERVICE_TYPE = 'billing'

    def _init(self):
        """
        Initialize the attributes.
        """
        pass

    def _init_workers(self):
        """
        Create the different worker instances.

        :returns: The list of these workers.
        """
        return [BillingWorker(self.conf, logger=self.logger)]
