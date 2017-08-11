from wing_module import Module


class Media(Module):
    FILESYSTEM = 'filesystem'
    CLOUDINARY = 'cloudinary'

    def init(self, config):
        #  Load and configure the media storage
        storage = config.get('storage', self.FILESYSTEM)
        self.storage = None
        klass = None

        if storage == self.FILESYSTEM:
            from .storage._filesystem import Filesystem
            klass = Filesystem

        elif storage == self.CLOUDINARY:
            from .storage._cloudinary import Cloudinary
            klass = Cloudinary

        if klass is not None:
            self.storage = klass(self.app, **config)

        self.storage.init()

    def put(self, container, fd, metadata={}):
        return self.storage.put(container, fd, metadata)

    def get(self, container, key):
        return self.storage.get(container, key)

    def get_url(self, container, key):
        return self.storage.get_url(container, key)

    def delete(self, container, key):
        self.storage.delete(container, key)

    def list(self, container):
        return self.storage.list(container)
