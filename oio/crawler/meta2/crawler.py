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


from oio.common.constants import STRLEN_REFERENCEID
from oio.common.exceptions import InvalidPath
from oio.crawler.common.crawler import VolumeCrawler, VolumeCrawlerWorker
from oio.crawler.meta2.meta2db import Meta2DB


class Meta2Worker(VolumeCrawlerWorker):
    """
    Worker responsible for listing and processing meta2 database files
    for a single meta2 volume.
    """

    SERVICE_TYPE = 'meta2'

    def cb(self, status, msg):
        if 500 <= status <= 599:
            self.logger.warning('Meta2worker volume %s handling failure %s',
                                self.volume, msg)

    def _process_path(self, path):
        """
        Perform the processing of a meta2 database file.

        :param path: The meta2 database file path.
        :returns: `True` if the meta2 database file has been processed.
        """
        db_id = path.rsplit("/")[-1].rsplit(".")
        if len(db_id) != 3:
            self.logger.warning("Malformed db file name: %s", path)
            raise InvalidPath
        if db_id[2] != 'meta2':
            self.logger.warning("Bad extension filename: %s", path)
            raise InvalidPath

        cid_seq = ".".join([db_id[0], db_id[1]])
        if len(cid_seq) < STRLEN_REFERENCEID:
            self.logger.warning('Not a valid CID: %s', cid_seq)
            raise InvalidPath

        meta2db = Meta2DB(self.app_env, dict())
        meta2db.path = path
        meta2db.volume_id = self.volume_id
        meta2db.cid = db_id[0]
        try:
            meta2db.seq = int(db_id[1])
        except ValueError:
            self.logger.warning('Bad sequence number: %s', db_id[1])
            raise InvalidPath

        try:
            self.pipeline(meta2db.env, self.cb)
            self.successes += 1
        except Exception:
            self.errors += 1
            self.logger.exception('Failed to apply pipeline')
        self.scanned_since_last_report += 1
        return True


class Meta2Crawler(VolumeCrawler):
    """
    Daemon responsible for multiple workers who will list
    and process meta2 database files for each meta2 volume.
    """

    SERVICE_TYPE = 'meta2'

    def _init_workers(self):
        """
        Create the different worker instances.

        :returns: The list of these workers.
        """
        return [
            Meta2Worker(self.conf, volume, logger=self.logger, api=self.api)
            for volume in self.volumes]
