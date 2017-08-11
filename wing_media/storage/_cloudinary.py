import cloudinary
import cloudinary.uploader
import cloudinary.api

import uuid


class Cloudinary(object):
    def __init__(self, app, **config):
        self.app = app

        self.base_url = config.get('base_url', '/media')
        self.max_depth = config.get('max_depth', 6)

        self.cloud_name = config.get('cloudinary_cloud_name')
        self.api_key = config.get('cloudinary_api_key')
        self.api_secret = config.get('cloudinary_api_secret')

    def init(self):
        cloudinary.config(
              cloud_name=self.cloud_name,
              api_key=self.api_key,
              api_secret=self.api_secret
        )

    def put(self, container, fd, metadata=None):
        key = uuid.uuid4().hex
        res = cloudinary.uploader.upload(
            fd,
            public_id='/'.join((container, key))
        )
        version = res['version']
        format = res['format']
        key = ';'.join((str(version), key, format))

        return key

    def get(self, container, key):
        raise NotImplementedError

    def delete(self, container, key):
        cloudinary.uploader.destroy(key)

    def get_url(self, container, key):
        version, key, format = key.split(';')
        return (
            'http://res.cloudinary.com/{cloud_name}/image/upload/v{version}'
            '/{container}/{key}.{format}'
        ).format(
            cloud_name=self.cloud_name,
            version=version,
            container=container,
            key=key,
            format=format
        )
