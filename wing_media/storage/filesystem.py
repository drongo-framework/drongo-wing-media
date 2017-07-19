from drongo import HttpResponseHeaders

from functools import partial
from datetime import datetime, timedelta

import mimetypes
import os


class Filesystem(object):
    def __init__(self, **config):
        self.path = config.get('path')
        self.age = 300

    def save(self, path, fd):
        fpath = os.path.join(self.path, path)

        if not os.path.exists(os.path.dirname(fpath)):
            os.makedirs(os.path.dirname(fpath))

        with open(fpath, 'wb') as wfd:
            for chunk in iter(partial(fd.read, 102400), b''):
                wfd.write(chunk)

    def chunks(self, path):
        with open(path, 'rb') as fd:
            for chunk in iter(partial(fd.read, 102400), b''):
                yield chunk

    def serve(self, path, ctx):
        path = os.path.join(self.path, path)

        ctx.response.set_header(HttpResponseHeaders.CACHE_CONTROL,
                                'max-age=%d' % self.age)

        expires = datetime.utcnow() + timedelta(seconds=(self.age))
        expires = expires.strftime('%a, %d %b %Y %H:%M:%S GMT')
        ctx.response.set_header(HttpResponseHeaders.EXPIRES, expires)

        ctype = 'application/octet-stream'
        ctx.response.set_header(HttpResponseHeaders.CONTENT_TYPE, ctype)
        ctx.response.set_content(self.chunks(path), os.stat(path).st_size)
