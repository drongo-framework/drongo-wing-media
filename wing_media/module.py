from wing_module import Module

import logging


__all__ = ['Media']


class Media(Module):
    """Drongo module that manages media"""

    FILESYSTEM = 'filesystem'
    CLOUDINARY = 'cloudinary'
    AWS_S3 = 'aws_s3'

    __default_config__ = {
        'storage': FILESYSTEM,
    }

    logger = logging.getLogger('wing_media')

    def init(self, config):
        self.logger.info('Initializing [media] module.')
        self.app.context.modules.media = self

        #  Load and configure the media storage
        storage = config.storage
        self.storage = None
        klass = None

        if storage == self.FILESYSTEM:
            from .storage._filesystem import Filesystem
            klass = Filesystem

        elif storage == self.CLOUDINARY:
            from .storage._cloudinary import Cloudinary
            klass = Cloudinary

        elif storage == self.AWS_S3:
            from .storage._aws_s3 import AWSS3
            klass = AWSS3

        if klass is not None:
            self.storage = klass(self.app, **config)
        else:
            self.logger.error('Cannot load [media] module.')
            raise NotImplementedError

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
