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

from six import reraise
import json
import sys
import time
from oio.api.base import HttpApi
from oio.common.constants import TIMEOUT_KEYS
from oio.common.decorators import patch_kwargs
from oio.common.easy_value import float_value
from oio.common.exceptions import OioException, OioNetworkException
from oio.common.logger import get_logger
from oio.conscience.client import ConscienceClient


class BucketClient(HttpApi):
    """Simple client API for the bucket service."""

    def __init__(self, conf, endpoint=None, proxy_endpoint=None,
                 refresh_delay=3600.0, logger=None, **kwargs):
        """
        Initialize a client for the bucket service.

        :param conf: dictionary with at least the namespace name
        :type conf: `dict`
        :param endpoint: URL of an account service
        :param proxy_endpoint: URL of the proxy
        :param refresh_interval: time between refreshes of the
        account service endpoint (if not provided at instantiation)
        :type refresh_interval: `float` seconds
        """
        super(BucketClient, self).__init__(
            endpoint=endpoint, service_type='account-service', **kwargs)
        self.logger = logger or get_logger(conf)
        self.cs = ConscienceClient(conf, endpoint=proxy_endpoint,
                                   logger=self.logger, **kwargs)

        self._global_kwargs = {tok: float_value(tov, None)
                               for tok, tov in kwargs.items()
                               if tok in TIMEOUT_KEYS}

        self._refresh_delay = refresh_delay if not self.endpoint else -1.0
        self._last_refresh = 0.0

    def _get_account_addr(self, **kwargs):
        """Fetch IP and port of an account service from Conscience."""
        acct_instance = self.cs.next_instance('account', **kwargs)
        acct_addr = acct_instance.get('addr')
        return acct_addr

    def _refresh_endpoint(self, now=None, **kwargs):
        """Refresh account service endpoint."""
        addr = self._get_account_addr(**kwargs)
        self.endpoint = '/'. join(("http:/", addr, "v1.0/bucket"))
        if not now:
            now = time.time()
        self._last_refresh = now

    def _maybe_refresh_endpoint(self, **kwargs):
        """Refresh account service endpoint if delay has been reached."""
        if self._refresh_delay >= 0.0 or not self.endpoint:
            now = time.time()
            if now - self._last_refresh > self._refresh_delay:
                try:
                    self._refresh_endpoint(now, **kwargs)
                except OioNetworkException as exc:
                    if not self.endpoint:
                        # Cannot use the previous one
                        raise
                    self.logger.warn(
                            "Failed to refresh account endpoint: %s", exc)
                except OioException:
                    if not self.endpoint:
                        # Cannot use the previous one
                        raise
                    self.logger.exception("Failed to refresh account endpoint")

    # Since all operations implemented in this class (as of 2019-08-08) result
    # in only one request to the account service, we can patch the keyword
    # arguments here. If this is changed, put the decorator on each public
    # method of this class.
    @patch_kwargs
    def account_request(self, account, method, action, params=None, **kwargs):
        """Make a request to the account service."""
        self._maybe_refresh_endpoint(**kwargs)
        if not params:
            params = dict()
        if account:
            # Do not quote account, _request() will urlencode query string
            params['id'] = account
        try:
            resp, body = self._request(method, action, params=params, **kwargs)
        except OioNetworkException as exc:
            exc_info = sys.exc_info()
            if self._refresh_delay >= 0.0:
                self.logger.info(
                    "Refreshing account endpoint after error %s", exc)
                try:
                    self._refresh_endpoint(**kwargs)
                except Exception as exc:
                    self.logger.warn("%s", exc)
            reraise(exc_info[0], exc_info[1], exc_info[2])
        return resp, body

    def bucket_list(self, account, limit=None, marker=None,
                    prefix=None, **kwargs):
        """
        Get the list of buckets of an account.

        :param account: account from which to get the bucket list
        :type account: `str`
        :keyword limit: maximum number of results to return
        :type limit: `int`
        :keyword marker: name of the bucket from where to start the listing
        :type marker: `str`
        :keyword prefix:
        :rtype: `dict` with 'ctime' (`float`), 'buckets' (`int`),
            'bytes' (`int`), 'objects' (`int`), 'containers' (`int`),
            'id' (`str`), 'metadata' (`dict`), 'listing' (`list`),
            'truncated' and 'next_marker'.
            'listing' contains dicts of container metadata (name,
            number of objects, number of bytes and modification time).
         """
        params = {"id": account,
                  "limit": limit,
                  "marker": marker,
                  "prefix": prefix}
        _resp, body = self.account_request(account, 'GET', 'buckets',
                                           params=params, **kwargs)
        return body

    def bucket_show(self, bucket, **kwargs):
        """
        Get information about a bucket.
        """
        _resp, body = self.account_request(bucket, 'GET', 'show-bucket',
                                           **kwargs)
        return body

    def bucket_update(self, bucket, metadata, to_delete, **kwargs):
        """
        Update metadata of the specified bucket.

        :param metadata: dictionary of properties that must be set or updated.
        :type metadata: `dict`
        :param to_delete: list of property keys that must be removed.
        :type to_delete: `list`
        """
        data = json.dumps({"metadata": metadata, "to_delete": to_delete})
        _resp, body = self.account_request(bucket, 'PUT', 'update-bucket',
                                           data=data, **kwargs)
        return body

    def bucket_refresh(self, bucket, **kwargs):
        """
        Refresh the counters of a bucket. Recompute them from the counters
        of all shards (containers).
        """
        self.account_request(bucket, 'POST', 'refresh-bucket', **kwargs)

    def bucket_reserve(self, bucket, **kwargs):
        """
        Reserve the bucket name during bucket creation.
        """
        data = json.dumps({'account': kwargs.get('owner')})
        _resp, body = self.account_request(bucket, 'PUT', 'reserve-bucket',
                                           data=data, **kwargs)
        return body

    def bucket_release(self, bucket, **kwargs):
        """
        Release the bucket reservration after success.
        """
        self.account_request(bucket, 'POST', 'release-bucket', **kwargs)

    def set_bucket_owner(self, bucket, **kwargs):
        """
        Set the bucket owner during reservation.
        """
        data = json.dumps({'account': kwargs.get('owner')})
        _resp, body = self.account_request(bucket, 'PUT', 'set-bucket-owner',
                                           data=data, **kwargs)
        return body

    def get_bucket_owner(self, bucket, **kwargs):
        """
        Get the bucket owner.
        """
        _resp, body = self.account_request(bucket, 'GET', 'get-bucket-owner',
                                           **kwargs)
        return body
