from boto3.session import Session

import uuid


class AWSS3(object):
    def __init__(self, app, **config):
        self.app = app

        self.base_url = config.get('base_url', '/media')
        self.max_depth = config.get('max_depth', 6)

        self.api_key = config.get('aws_s3_key')
        self.api_secret = config.get('aws_s3_secret')
        self.api_region = config.get('aws_s3_region')
        self.bucket_name = config.get('aws_s3_bucket')

    def init(self):
        session = Session(
            aws_access_key_id=self.api_key,
            aws_secret_access_key=self.api_secret,
            region_name=self.api_region)
        self.conn = session.resource('s3')
        self.bucket = self.conn.Bucket(self.bucket_name)

    def put(self, container, fd, metadata=None):
        key = uuid.uuid4().hex
        s3_key = '/'.join((container, key))
        self.bucket.upload_fileobj(fd, s3_key)
        return key

    def get(self, container, key):
        raise NotImplementedError

    def delete(self, container, key):
        s3_key = '/'.join((container, key))
        obj = self.bucket.Object(s3_key)
        obj.delete()

    def get_url(self, container, key):
        return (
            'https://s3.{region}.amazonaws.com/{bucket_name}/{container}/{key}'
        ).format(
            region=self.api_region,
            bucket_name=self.bucket_name,
            container=container,
            key=key
        )
