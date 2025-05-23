# Copyright (C) 2015-2019 OpenIO SAS, as part of OpenIO SDS
# Copyright (C) 2021-2025 OVH SAS
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

import re
from urllib.parse import quote

from oio.common.constants import CHUNK_HEADERS
from oio.content.quality import location_constraint_margin

_TOKEN = r"[^()<>@,;:\"/\[\]?={}\x00-\x20\x7f]+"
_EXT_PATTERN = re.compile(
    r"(?:\s*;\s*(" + _TOKEN + r")\s*(?:=\s*(" + _TOKEN + r'|"(?:[^"\\]|\\.)*"))?)'
)


def parse_content_type(raw_content_type):
    param_list = []
    if raw_content_type:
        if ";" in raw_content_type:
            _content_type, params = raw_content_type.split(";", 1)
            params = ";" + params
            for match in _EXT_PATTERN.findall(params):
                k = match[0].strip()
                v = match[1].strip()
                param_list.append((k, v))
    return raw_content_type, param_list


_content_range_pattern = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")


def parse_content_range(raw_content_range):
    found = re.search(_content_range_pattern, raw_content_range)
    if not found:
        raise ValueError("invalid content-range %r" % (raw_content_range,))
    return tuple(int(x) for x in found.groups())


def http_header_from_ranges(ranges):
    header = "bytes="
    for i, (start, end) in enumerate(ranges):
        if end:
            if end < 0:
                raise ValueError("Invalid range (%s, %s)" % (start, end))
            elif start is not None and end < start:
                raise ValueError("Invalid range (%s, %s)" % (start, end))
        else:
            if start is None:
                raise ValueError("Invalid range (%s, %s)" % (start, end))

        if start is not None:
            header += str(start)
        header += "-"

        if end is not None:
            header += str(end)
        if i < len(ranges) - 1:
            header += ","
    return header


def ranges_from_http_header(val):
    if not val.startswith("bytes="):
        raise ValueError("Invalid Range value: %s" % val)
    ranges = []
    for rng in val[6:].split(","):
        start, end = rng.split("-", 1)
        if start:
            start = int(start)
        else:
            start = None
        if end:
            end = int(end)
            if end < 0:
                raise ValueError("Invalid byterange value: %s" % val)
            elif start is not None and end < start:
                raise ValueError("Invalid byterange value: %s" % val)
        else:
            end = None
            if start is None:
                raise ValueError("Invalid byterange value: %s" % val)
        ranges.append((start, end))
    return ranges


def headers_from_object_metadata(metadata, chunk_url):
    """
    Generate chunk PUT request headers from object metadata.
    """
    headers = {}
    headers["transfer-encoding"] = "chunked"
    # FIXME: remove key incoherencies
    headers[CHUNK_HEADERS["content_id"]] = metadata["id"]
    headers[CHUNK_HEADERS["content_version"]] = str(metadata["version"])
    headers[CHUNK_HEADERS["content_path"]] = quote(metadata["content_path"])
    headers[CHUNK_HEADERS["content_chunkmethod"]] = metadata["chunk_method"]
    headers[CHUNK_HEADERS["content_policy"]] = metadata["policy"]
    headers[CHUNK_HEADERS["container_id"]] = metadata["container_id"]

    for key in ("metachunk_hash", "metachunk_size", "chunk_hash"):
        val = metadata.get(key)
        if val is not None:
            headers[CHUNK_HEADERS[key]] = str(metadata[key])

    headers[CHUNK_HEADERS["full_path"]] = metadata["full_path"]

    # If chunk quality is not good enough, add <non_optimal_placement> header.
    try:
        qual = metadata["qualities"][chunk_url]
        if not qual:
            raise KeyError

        # Check with warn_dist (old way)
        if (
            qual["final_dist"] <= qual["warn_dist"]
            or qual["expected_slot"] != qual["final_slot"]
        ):
            headers[CHUNK_HEADERS["non_optimal_placement"]] = True

        # Check with fair_location_constraint (new way!)
        if location_constraint_margin(qual)[0] < 0:
            headers[CHUNK_HEADERS["non_optimal_placement"]] = True
    except KeyError:
        # Ignore if there is no qualities.
        pass

    return headers


def get_addr(host, port):
    """
    Generate the address for host (IPv4 or IPv6) and port
    """
    if ":" in host:  # IPv6
        return "[%s]:%s" % (host, port)
    else:  # IPv4
        return "%s:%s" % (host, port)


class HeadersDict(dict):
    def __init__(self, headers, **kwargs):
        if headers:
            self.update(headers)
        self.update(kwargs)

    def update(self, data):
        if hasattr(data, "keys"):
            for key in data.keys():
                self[key.title()] = data[key]
        else:
            for k, v in data:
                self[k.title()] = v

    def __delitem__(self, k):
        return dict.__delitem__(self, k.title())

    def __getitem__(self, k):
        return dict.__getitem__(self, k.title())

    def __setitem__(self, k, v):
        if v is None:
            self.pop(k.title(), None)
        return dict.__setitem__(self, k.title(), v)

    def get(self, k, default=None):
        return dict.get(self, k.title(), default)

    def pop(self, k, default=None):
        return dict.pop(self, k.title(), default)
