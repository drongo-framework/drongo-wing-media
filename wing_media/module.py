import os


class Media(object):
    def __init__(self, app, **config):
        self.app = app

        self.base_url = config.get('base_url', '/media')

        # Load and configure the session storage
        storage = config.get('storage', 'filesystem')
        self.storage = None
        if storage == 'filesystem':
            from .storage.filesystem import Filesystem
            path = config.get('media_path', './.media')
            self.storage = Filesystem(path=path)

        self.age = config.get('age', 300)
        self.max_depth = config.get('max_depth', 6)

        self.init()

    def init(self):
            parts = ['', '{a}', '{b}', '{c}', '{d}', '{e}', '{f}']
            for i in range(2, self.max_depth + 2):
                self.app.add_url(
                    pattern=self.base_url + '/'.join(parts[:i]),
                    call=self.serve_file)

    def serve_file(self, ctx,
                   a=None, b=None, c=None, d=None, e=None, f=None):
        path = ''
        parts = [a, b, c, d, e, f]
        for part in parts:
            if part is not None:
                path = os.path.join(path, part)
        return self.storage.serve(path, ctx)

    def save(self, path, fd):
        self.storage.save(path, fd)

    def get_url(self, media_path):
        return '/'.join((self.base_url, media_path))

    def chunks(self, path):
        with open(path, 'rb') as fd:
            for chunk in iter(partial(fd.read, 102400), b''):
                yield chunk
