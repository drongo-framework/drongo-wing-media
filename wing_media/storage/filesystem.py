from drongo import HttpResponseHeaders

from datetime import datetime, timedelta
from functools import partial

import json
import os
import uuid


class Filesystem(object):
    def __init__(self, **config):
        self.path = config.get('path')

    def init(self):
        pass  # Nothing to be done here!

    def _normalize_container(self, value):
        return value.replace('/', '__')

    def put(self, container, fd, metadata):
        container = self._normalize_container(container)

        folder = os.path.join(self.path, container)
        if not os.path.exists(folder):
            os.makedirs(folder)

        while True:
            key = uuid.uuid4().hex
            fpath = os.path.join(folder, key)
            if not os.path.exists(fpath):
                break

        size = 0
        with open(fpath, 'wb') as wfd:
            for chunk in iter(partial(fd.read, 102400), b''):
                wfd.write(chunk)
                size += len(chunk)

        metadata['size'] = size
        metadata['physical_path'] = fpath
        with open(fpath + '.meta', 'w') as mfd:
            json.dump(metadata, mfd)

        return key

    def get(self, container, key):
        container = self._normalize_container(container)
        folder = os.path.join(self.path, container)
        fpath = os.path.join(folder, key)

        if os.path.exists(fpath):
            with open(fpath + '.meta') as mfd:
                return open(fpath, 'rb'), json.load(mfd)

        raise FileNotFoundError

    def delete(self, container, key):
        container = self._normalize_container(container)
        folder = os.path.join(self.path, container)
        fpath = os.path.join(folder, key)

        if os.path.exists(fpath):
            os.unlink(fpath)
            os.unlink(fpath + '.meta')
            return

        raise FileNotFoundError

    def list(self, container):
        raise NotImplementedError
