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

from oio.blob.utils import read_chunk_metadata
from oio.common.exceptions import FaultyChunk, InvalidPath, MissingAttribute
from oio.common.utils import is_chunk_id_valid
from oio.crawler.common.crawler import VolumeCrawler, VolumeCrawlerWorker
from oio.crawler.rawx.chunk_wrapper import ChunkWrapper, is_success, is_error


class RawxWorker(VolumeCrawlerWorker):
    """
    Worker responsible for listing and processing chunk files
    for a single rawx volume.
    """

    SERVICE_TYPE = 'rawx'

    def cb(self, status, msg):
        if is_success(status):
            pass
        elif is_error(status):
            self.logger.warning('Rawx volume_id=%s handling failure: %s',
                                self.volume_id, msg)
        else:
            self.logger.warning('Rawx volume_id=%s status=%d msg=%s',
                                self.volume_id, status, msg)

    def _process_path(self, path):
        """
        Perform the processing of a chunk file.

        :param path: The chunk file path.
        :returns: `True` if the chunk file has been processed.
        """
        chunk = ChunkWrapper({})
        chunk.chunk_id = path.rsplit('/', 1)[-1]
        chunk.chunk_path = path

        try:
            if not is_chunk_id_valid(chunk.chunk_id):
                self.logger.info('Skip not valid chunk path %s',
                                 chunk.chunk_path)
                raise InvalidPath
            with open(chunk.chunk_path, 'rb') as chunk_file:
                # A supposition is made: metadata will not change during the
                # process of all filters
                chunk.meta, _ = read_chunk_metadata(chunk_file, chunk.chunk_id)
        except (MissingAttribute, FaultyChunk):
            self.logger.error('Skip not valid chunk %s', chunk.chunk_path)
            self.errors += 1
            return False

        try:
            self.pipeline(chunk.env, self.cb)
            self.successes += 1
        except Exception:
            self.errors += 1
            self.logger.exception('Failed to apply pipeline')
        self.scanned_since_last_report += 1
        return True


class RawxCrawler(VolumeCrawler):
    """
    Daemon responsible for multiple workers who will list
    and process chunk files for each rawx volume.
    """

    SERVICE_TYPE = 'rawx'

    def _init_workers(self):
        """
        Create the different worker instances.

        :returns: The list of these workers.
        """
        return [
            RawxWorker(self.conf, volume, logger=self.logger, api=self.api)
            for volume in self.volumes]
