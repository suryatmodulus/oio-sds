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

from oio.common.constants import TIMEOUT_KEYS
from oio.common.easy_value import float_value
from oio.account.base_client import BaseClient


class IamClient(BaseClient):
    """Simple client API for the Iam service."""

    def __init__(self, conf, endpoint=None, proxy_endpoint=None,
                 refresh_delay=3600.0, logger=None, **kwargs):
        """
        Initialize a client for the iam service.

        :param conf: dictionary with at least the namespace name
        :type conf: `dict`
        :param endpoint: URL of an account service
        :param proxy_endpoint: URL of the proxy
        :param refresh_interval: time between refreshes of the
        account service endpoint (if not provided at instantiation)
        :type refresh_interval: `float` seconds
        """
        super(IamClient, self).__init__(
            conf=conf, endpoint=endpoint, proxy_endpoint=proxy_endpoint,
            refresh_delay=refresh_delay, logger=logger, **kwargs)
        self._global_kwargs = {tok: float_value(tov, None)
                               for tok, tov in kwargs.items()
                               if tok in TIMEOUT_KEYS}

    def _account_api_endpoint(self):
        return 'iam'

    def iam_delete_user_policy(self, account, user, policy, **kwargs):
        params = {"user": user,
                  "policiy-name": policy}
        _resp, body = self.account_request(account, 'DELETE',
                                           'delete-user-policy',
                                           params=params,
                                           **kwargs)
        return _resp

    def iam_get_user_policy(self, account, user, **kwargs):
        params = {"account": account,
                  "user": user}
        _resp, body = self.account_request(account, 'GET',
                                           'get-user-policy',
                                           params=params,
                                           **kwargs)
        return body

    def iam_list_users(self, account, **kwargs):
        _resp, body = self.account_request(account, 'GET',
                                           'list-users',
                                           **kwargs)
        return body

    def iam_list_user_policies(self, account, user, **kwargs):
        params = {"account": account,
                  "user": user}
        _resp, body = self.account_request(account, 'GET',
                                           'list-user-policies',
                                           params=params,
                                           **kwargs)
        return body

    def iam_load_merged_user_policies(self, account, user, **kwargs):
        """
        load merged policies for given couple account/user

        :param account: name of the account
        :type account: `str`
        :param user: user of account
        :type user: `str`
        """
        params = {"account": account,
                  "user": user}
        _resp, body = self.account_request(account, 'GET',
                                           'load-merged-user-policies',
                                           params=params,
                                           **kwargs)
        return body

    def iam_put_user_policy(self, account, user, policy, data, **kwargs):
        params = {"account": account,
                  "user": user,
                  "policy-name": policy}
        _resp, body = self.account_request(account, 'PUT',
                                           'put-user-policy',
                                           params=params,
                                           data=data,
                                           **kwargs)
        return body
