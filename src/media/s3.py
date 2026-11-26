"""S3-compatible storage and path-style links for connector media."""

import os
from typing import Any, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.media.config import MediaConfig
from src.media.store import ListedObject, Listing, ObjectTooLarge, StoredObject

MISSING = {"404", "NoSuchKey", "NotFound"}
ACCESS_KEY_VARIABLE = "MIROIR_MEDIA_ACCESS_KEY_ID"
SECRET_KEY_VARIABLE = "MIROIR_MEDIA_SECRET_ACCESS_KEY"


def from_environment(config: MediaConfig) -> "S3ObjectStore | None":
    """The bucket of the `media` section, with the key from the environment;
    None when either half of the key is not set."""
    access_key = os.environ.get(ACCESS_KEY_VARIABLE)
    secret_key = os.environ.get(SECRET_KEY_VARIABLE)
    if not access_key or not secret_key:
        return None
    return S3ObjectStore(
        config.endpoint,
        config.link_endpoint or config.endpoint,
        config.region,
        config.bucket,
        access_key,
        secret_key,
    )


class S3ObjectStore:
    def __init__(
        self,
        endpoint: str,
        link_endpoint: str,
        region: str,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
    ) -> None:
        self.bucket = bucket
        options = {
            "region_name": region,
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_access_key,
            "config": Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=5,
                read_timeout=10,
                retries={"max_attempts": 2},
            ),
        }
        self.client = boto3.client("s3", endpoint_url=endpoint, **options)
        self.link_client = boto3.client("s3", endpoint_url=link_endpoint, **options)

    def head(self, name: str) -> int | None:
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=name)
            return cast(int, response["ContentLength"])
        except ClientError as error:
            if error.response["Error"]["Code"] in MISSING:
                return None
            raise

    def get(self, name: str, max_bytes: int) -> StoredObject | None:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=name)
        except ClientError as error:
            if error.response["Error"]["Code"] in MISSING:
                return None
            raise
        body = response["Body"]
        try:
            if response.get("ContentLength", 0) > max_bytes:
                raise ObjectTooLarge(f"Stored object exceeds {max_bytes} bytes")
            # The declared length is checked; reading one byte past it checks the body.
            data = body.read(max_bytes + 1)
        finally:
            body.close()
        if len(data) > max_bytes:
            raise ObjectTooLarge(f"Stored object exceeds {max_bytes} bytes")
        return StoredObject(data, response.get("ContentType"), response.get("ETag"))

    def put(self, name: str, body: bytes, content_type: str) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=name, Body=body, ContentType=content_type
        )

    def list_objects(self, prefix: str, after: str, limit: int) -> Listing:
        options: dict[str, Any] = {"StartAfter": after} if after else {}
        response = self.client.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, MaxKeys=min(limit, 1000), **options
        )
        return Listing(
            [
                ListedObject(item["Key"], int(item["Size"]), item["LastModified"])
                for item in response.get("Contents", [])
            ],
            bool(response.get("IsTruncated", False)),
        )

    def presign_put(self, name: str, content_type: str, expires: int) -> str:
        url = self.link_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": self.bucket, "Key": name, "ContentType": content_type},
            ExpiresIn=expires,
        )
        return cast(str, url)

    def presign_get(self, name: str, expires: int) -> str:
        url = self.link_client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": name}, ExpiresIn=expires
        )
        return cast(str, url)
