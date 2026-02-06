from __future__ import annotations

import gzip
import io
import json
import logging
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from app.config import settings

logger = logging.getLogger(__name__)


class S3Storage:
    """
    S3 artifact storage layer.

    Rules:
    - S3 stores artifacts only (raw / parsed / processed)
    - Snowflake is the source of truth for state
    - Supports both plain text and gzip transparently
    - Never reconstructs logical state from S3
    """

    def __init__(self) -> None:
        bucket = settings.S3_BUCKET_NAME
        if not bucket:
            raise ValueError("S3 bucket not configured. Set S3_BUCKET_NAME (or S3_BUCKET).")

        self.bucket = bucket
        self.prefix = ""  # no prefix support in current Settings

        self.client = boto3.client(
            "s3",
            region_name=settings.AWS_REGION or "us-east-1",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )

    def _full_key(self, key: str) -> str:
        key = key.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{key}"
        return key

    def put_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> str:
        full_key = self._full_key(key)

        extra = {}
        if content_type:
            extra["ContentType"] = content_type

        self.client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=data,
            **extra,
        )
        return full_key

    def put_text(self, key: str, text: str, gzip_compress: bool = False) -> str:
        if gzip_compress:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                gz.write(text.encode("utf-8", errors="ignore"))
            return self.put_bytes(key, buf.getvalue(), content_type="text/plain")

        return self.put_bytes(
            key=key,
            data=text.encode("utf-8", errors="ignore"),
            content_type="text/plain",
        )

    def put_json(self, key: str, obj: Dict[str, Any], gzip_compress: bool = False) -> str:
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8", errors="ignore")

        if gzip_compress:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                gz.write(payload)
            return self.put_bytes(key, buf.getvalue(), content_type="application/json")

        return self.put_bytes(key=key, data=payload, content_type="application/json")

    def exists(self, key: str) -> bool:
        full_key = self._full_key(key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def get_bytes(self, key: str) -> bytes:
        full_key = self._full_key(key)
        resp = self.client.get_object(Bucket=self.bucket, Key=full_key)
        return resp["Body"].read()

    def read_text_auto(self, key: str) -> str:
        data = self.get_bytes(key)

        is_gz = key.endswith(".gz") or (len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B)

        if is_gz:
            try:
                return gzip.decompress(data).decode("utf-8", errors="ignore")
            except Exception:
                logger.warning("Failed gzip decode for %s, falling back to raw decode", key)
                return data.decode("utf-8", errors="ignore")

        return data.decode("utf-8", errors="ignore")

    def read_json_auto(self, key: str) -> Dict[str, Any]:
        data = self.get_bytes(key)

        is_gz = key.endswith(".gz") or (len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B)

        if is_gz:
            try:
                data = gzip.decompress(data)
            except Exception:
                logger.warning("Failed gzip decode for %s, using raw bytes", key)

        return json.loads(data.decode("utf-8", errors="ignore"))
