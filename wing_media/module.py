import os


class Media(object):
    def __init__(self, app, **config):
        self.app = app

        # Load and configure the session storage
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

    def put(self, container, fd, metadata={}):
        return self.storage.save(container, fd, metadata)

    def get(self, container, key):
        return self.storage.get(container, key)

    def delete(self, container, key):
        self.storage.delete(container, key)

    def list(self, container):
        return self.storage.list(container)

    # helper functions
    def serve(self, ctx, container, key):
        fd, meta = self.get(container, key)
        raise NotImplementedError
