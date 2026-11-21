"""S3-compatible storage and path-style links for connector media."""

from typing import cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.core.media import ObjectTooLarge, StoredObject

MISSING = {"404", "NoSuchKey", "NotFound"}


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
