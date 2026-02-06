from __future__ import annotations

import gzip
import json
import mimetypes
from dataclasses import dataclass
from typing import Any, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.config import settings


@dataclass
class S3Storage:
    """
    Minimal, production-style S3 wrapper.

    Required by pipelines:
      - get_bytes(bucket, key) -> bytes
      - put_bytes(bucket, key, data, content_type=...) -> None
      - exists(bucket, key) -> bool
      - list_keys(bucket, prefix, max_keys=...) -> List[str]

    Case Study 2 helpers:
      - put_json_gz(key, payload) / get_json_gz(key)
      - put_text_gz(key, text) / get_text_gz(key)
    """

    s3_client: Any
    bucket: str

    @classmethod
    def from_env(cls) -> "S3Storage":
        region = getattr(settings, "AWS_REGION", None) or getattr(settings, "AWS_DEFAULT_REGION", None) or "us-east-1"
        bucket = getattr(settings, "S3_BUCKET", None) or getattr(settings, "AWS_S3_BUCKET", None) or getattr(
            settings, "S3_BUCKET_NAME", None
        )
        if not bucket:
            raise RuntimeError(
                "Missing S3 bucket setting. Set S3_BUCKET (or AWS_S3_BUCKET / S3_BUCKET_NAME) in .env / settings."
            )

        access_key = getattr(settings, "AWS_ACCESS_KEY_ID", None)
        secret_key = getattr(settings, "AWS_SECRET_ACCESS_KEY", None)
        session_token = getattr(settings, "AWS_SESSION_TOKEN", None)

        client_kwargs: dict[str, Any] = {
            "region_name": region,
            "config": Config(retries={"max_attempts": 12, "mode": "adaptive"}, connect_timeout=30, read_timeout=120),
        }
        if access_key and secret_key:
            client_kwargs["aws_access_key_id"] = access_key
            client_kwargs["aws_secret_access_key"] = secret_key
            if session_token:
                client_kwargs["aws_session_token"] = session_token

        s3 = boto3.client("s3", **client_kwargs)
        return cls(s3_client=s3, bucket=bucket)

    # ---------------------------
    # Base primitives
    # ---------------------------
    def get_bytes(self, *, bucket: Optional[str] = None, key: str) -> bytes:
        b = bucket or self.bucket
        try:
            resp = self.s3_client.get_object(Bucket=b, Key=key)
            return resp["Body"].read()
        except ClientError as e:
            raise RuntimeError(f"S3 get_object failed bucket={b} key={key}: {e}") from e

    def put_bytes(
        self,
        *,
        bucket: Optional[str] = None,
        key: str,
        data: bytes,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
    ) -> None:
        b = bucket or self.bucket
        ct = content_type
        if not ct:
            ct, _ = mimetypes.guess_type(key)
        if not ct:
            ct = "application/octet-stream"

        kwargs: dict[str, Any] = {"Bucket": b, "Key": key, "Body": data, "ContentType": ct}
        if content_encoding:
            kwargs["ContentEncoding"] = content_encoding

        try:
            self.s3_client.put_object(**kwargs)
        except ClientError as e:
            raise RuntimeError(f"S3 put_object failed bucket={b} key={key}: {e}") from e

    def exists(self, *, bucket: Optional[str] = None, key: str) -> bool:
        b = bucket or self.bucket
        try:
            self.s3_client.head_object(Bucket=b, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def list_keys(self, *, bucket: Optional[str] = None, prefix: str, max_keys: int = 1000) -> List[str]:
        b = bucket or self.bucket
        keys: List[str] = []
        token = None

        while True:
            kwargs: dict[str, Any] = {"Bucket": b, "Prefix": prefix, "MaxKeys": min(max_keys, 1000)}
            if token:
                kwargs["ContinuationToken"] = token

            resp = self.s3_client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                keys.append(obj["Key"])
                if len(keys) >= max_keys:
                    return keys

            if not resp.get("IsTruncated"):
                return keys
            token = resp.get("NextContinuationToken")

    # ---------------------------
    # Case Study 2 helpers
    # ---------------------------
    def put_json_gz(self, *, key: str, payload: dict[str, Any], bucket: Optional[str] = None) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        gz = gzip.compress(raw, compresslevel=6)
        self.put_bytes(
            bucket=bucket,
            key=key,
            data=gz,
            content_type="application/json",
            content_encoding="gzip",
        )

    def get_json_gz(self, *, key: str, bucket: Optional[str] = None) -> dict[str, Any]:
        data = self.get_bytes(bucket=bucket, key=key)
        raw = gzip.decompress(data)
        return json.loads(raw.decode("utf-8", errors="ignore"))

    def put_text_gz(self, *, key: str, text: str, bucket: Optional[str] = None) -> None:
        raw = text.encode("utf-8", errors="ignore")
        gz = gzip.compress(raw, compresslevel=6)
        self.put_bytes(
            bucket=bucket,
            key=key,
            data=gz,
            content_type="text/plain",
            content_encoding="gzip",
        )

    def get_text_gz(self, *, key: str, bucket: Optional[str] = None) -> str:
        data = self.get_bytes(bucket=bucket, key=key)
        raw = gzip.decompress(data)
        return raw.decode("utf-8", errors="ignore")
