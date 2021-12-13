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

import json
from oio.common.constants import TIMEOUT_KEYS
from oio.common.easy_value import float_value
from oio.account.base_client import BaseClient


class BucketClient(BaseClient):
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
            conf=conf, endpoint=endpoint, proxy_endpoint=proxy_endpoint,
            refresh_delay=refresh_delay, logger=logger, **kwargs)

        self._global_kwargs = {tok: float_value(tov, None)
                               for tok, tov in kwargs.items()
                               if tok in TIMEOUT_KEYS}

    def _account_api_endpoint(self):
        return 'bucket'

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
