#!/usr/bin/env python

# Copyright (C) 2021 OVH SAS
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from oio import ObjectStorageApi

sds_namespace = 'OPENIO'
sds_proxy_url = 'http://127.0.0.1:6000'


def account():
    namespace = sds_namespace
    proxy_url = sds_proxy_url
    storage = ObjectStorageApi(namespace, endpoint=proxy_url)

    # accounts / buckets / metadata
    # list accounts
    accounts = storage.account.account_list()
    print(accounts)
    for acct in accounts:
        print(acct)
        account_info = storage.account.account_show(acct)
        print('account infos :', account_info)
        # for each account list containers
        containers = storage.account.container_list(acct)
        for el in containers['listing']:
            name = el[0]
            container_info = storage.account.container_show(acct, name)
            print(container_info)

    # for each account list buckets
    for acct in accounts:
        print(acct)
        # for each account list containers
        buckets = storage.account.bucket_list(acct)
        print('buckets:', buckets)
        for el in buckets['listing']:
            print('el bucket:', el)
            if el:
                name = el['name']
                bucket_info = storage.account.bucket_show(name)
                print(bucket_info)

def bucket_db():
    namespace = sds_namespace
    proxy_url = sds_proxy_url
    storage = ObjectStorageApi(namespace, endpoint=proxy_url)
    # list accounts
    accounts = storage.account.account_list()
    # for each account list buckets
    for acct in accounts:
        print(acct)
        # for each account list containers
        buckets = storage.account.bucket_list(acct)
        print('buckets:', buckets)
        for el in buckets['listing']:
            print('el bucket:', el)
            if el:
                name = el['name']
                bucket_info = storage.account.bucket_show(name)
                print(bucket_info)
                owner = storage.account.get_bucket_owner(name)
                # storage.account.set_bucket_owner(, owner=owner)

def iam():
    pass
if __name__ == '__main__':
    account()
    bucket_db()
    iam()


    # iam rules
    # do insert to fdb

    # bucket_db bucket _reservation
