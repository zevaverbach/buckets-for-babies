"""
TODO: write some tests, make sure all methods are working
TODO: write a README
TODO: support other AWS creds
TODO: publish to PyPI
"""
from collections.abc import MutableMapping
import os

import boto3

resource = boto3.resource("s3")
client = boto3.client("s3")


class DoesntExist(Exception):
    pass


class Failed(Exception):
    pass


class AlreadyExists(Exception):
    pass


class Bucket(MutableMapping):
    def __init__(self, name, create_if_doesnt_exist=False):
        self.name = name
        self.mapping = {}
        if not self.exists:
            if not create_if_doesnt_exist:
                raise DoesntExist(f"{name} doesn't exist!")
            self.bucket = Bucket.create(name).bucket
        else:
            self.bucket = resource.Bucket(name)
            self._populate_mapping(first=True)

    @property
    def exists(self):
        return self.name in list_bucket_names()

    def upload(self, filepath, key: str | None = None, metadata: dict | None = None):
        basename = os.path.basename(filepath)
        metadata = metadata or {}
        key = key or basename
        self.bucket.upload_file(Filename=filepath, Key=key, ExtraArgs={"Metadata": metadata})

    def search(self, keyword: str | None = None):
        keyword = keyword or ""
        return [o.key for o in self.bucket.objects.filter(Prefix=keyword)]

    @classmethod
    def create(cls, name):
        if name in list_bucket_names():
            raise AlreadyExists(f"bucket {name} already exists!")
        response = client.create_bucket(Bucket=name)
        response_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if not str(response_code).startswith("2"):
            raise Failed(f"response was {response_code}")
        return cls(name)

    def list_all_items(self):
        return self.search()

    def download_file(self, filename):
        self.bucket.download_file(filename, filename)

    def delete_all_files(self):
        for f in self.values():
            f.delete()

    def __repr__(self):
        return f"<Bucket {self.name}>"

    def __iter__(self):
        return iter(self.mapping)

    def __len__(self):
        return len(self.mapping)

    def __contains__(self, filename):
        if not self.mapping:
            self._populate_mapping()
        return filename in self.mapping

    def __getitem__(self, key):
        if key in self.mapping:
            s3_file = S3File(key, self)
            self[key] = s3_file
            return s3_file
        raise KeyError

    def __setitem__(self, key, value):
        if value is None or isinstance(value, S3File):
            self.mapping[key] = value
        else:
            # it's a filepath
            self.upload(value)
            self.mapping[key] = S3File(key, self)

    def __delitem__(self, key):
        value = self[key]
        value.delete()
        del self.mapping[key]

    def _populate_mapping(self, first=False):
        for item in self.list_all_items():
            if first or (not first and item not in self):
                self[item] = S3File(first, self)

    def copy(self):
        return self.mapping.copy()


class S3File:
    def __init__(self, filename: str, bucket: Bucket):
        self.filename = filename
        self.bucket = bucket

    def __repr__(self):
        return f"<S3File {self.filename} in {self.bucket.name}>"

    @property
    def obj(self):
        return resource.Object(self.bucket.name, self.filename)

    def delete(self):
        resource.Object(self.bucket.name, self.filename).delete()

    def download(self):
        self.bucket.bucket.download_file(self.filename)

    def restore_from_glacier(self):
        resource.meta.client.restore_object(
            Bucket=self.bucket.name, Key=self.filename, RestoreRequest={"Days": 1}
        )

    def generate_url(self, expires_in_seconds=3600):
        if not self.exists:
            raise FileNotFoundError

        return client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket.name,
                "Key": self.filename,
                "ResponseContentDisposition": f"attachment; filename={self.filename}",
            },
            ExpiresIn=expires_in_seconds,
        )

    def set_metadata(self, metadata: dict):
        self.bucket.bucket.Object(self.filename).put(Metadata=metadata)

    def upload(self, key: str | None = None, metadata: dict | None = None):
        self.bucket.upload(
            self.filename,
            metadata=metadata,
            key=key,
        )

    def get_attrib(self, attr):
        head_object = client.head_object(Bucket=self.bucket.bucket)

        return head_object[attr]

    def rename(self, new_name, delete=True):
        copy_source = {"Bucket": self.bucket.name, "Key": self.filename}
        resource.meta.client.copy(copy_source, self.bucket.name, new_name)

        if delete and self.exists():
            self.delete()

    def save_as(self, new_name):
        return self.rename(new_name)

    def copy_to_bucket(self, bucket_name, new_name: str | None = None):
        copy_source = {"Bucket": self.bucket.name, "Key": self.filename}

        resource.meta.client.copy(copy_source, bucket_name, new_name or self.filename)

    @property
    def valid(self):
        return not self.is_data_file

    @property
    def is_data_file(self):
        return "/" in self.filename

    @property
    def exists(self):
        return bool(len(self.bucket.search(keyword=self.filename)))

    def get_filesize(self):
        return self.get_attrib("ContentLength")

    def get_last_modified(self):
        return self.get_attrib("LastModified")

    def get_metadata(self):
        return self.get_attrib("Metadata")


def list_bucket_names():
    return [bucket["Name"] for bucket in client.list_buckets()["Buckets"]]
