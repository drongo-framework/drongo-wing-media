from drongo import HttpResponseHeaders

from datetime import datetime, timedelta
from functools import partial


class Media(object):
    def __init__(self, app, **config):
        self.app = app

        self.age = config.get('age', 300)
        self.base_url = config.get('base_url', '/media')
        self.max_depth = config.get('max_depth', 6)

        #  Load and configure the media storage
        storage = config.get('storage', 'filesystem')
        self.storage = None
        klass = None

        if storage == 'filesystem':
            from .storage.filesystem import Filesystem
            klass = Filesystem

        if klass is not None:
            self.storage = klass(**config)

        self.init()

    def init(self):
        self.storage.init()

        parts = ['', '{a}', '{b}', '{c}', '{d}', '{e}', '{f}']
        for i in range(2, self.max_depth + 2):
            self.app.add_url(
                pattern=self.base_url + '/'.join(parts[:i]),
                call=self._serve_media)

    def put(self, container, fd, metadata={}):
        return self.storage.put(container, fd, metadata)

    def get(self, container, key):
        return self.storage.get(container, key)

    def get_url(self, container, key):
        return '{base_url}/{container}/{key}'.format(
            base_url=self.base_url,
            container=container,
            key=key
        )

    def delete(self, container, key):
        self.storage.delete(container, key)

    def list(self, container):
        return self.storage.list(container)

    def _chunks(self, fd):
        for chunk in iter(partial(fd.read, 102400), b''):
            yield chunk
        fd.close()

    # helper functions
    def serve(self, ctx, container, key):
        fd, meta = self.get(container, key)
        ctype = meta.get('mimetype')
        size = meta.get('size')
        expires = datetime.utcnow() + timedelta(seconds=(self.age))
        expires = expires.strftime('%a, %d %b %Y %H:%M:%S GMT')

        ctx.response.set_header(HttpResponseHeaders.CACHE_CONTROL,
                                'max-age=%d' % self.age)
        ctx.response.set_header(HttpResponseHeaders.EXPIRES, expires)
        ctx.response.set_header(HttpResponseHeaders.CONTENT_TYPE, ctype)
        ctx.response.set_content(self._chunks(fd), size)

    def _serve_media(self, ctx,
                     a=None, b=None, c=None, d=None, e=None, f=None):
        parts = [a, b, c, d, e, f]
        while None in parts:
            parts.remove(None)

        container = '/'.join(parts[:-1])
        key = parts[-1]

        self.serve(ctx, container, key)
